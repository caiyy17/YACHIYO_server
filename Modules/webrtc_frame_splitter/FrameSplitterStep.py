import base64
import io
import json
import queue
import time
import wave
from collections import deque
from math import gcd

import numpy as np
from PIL import Image

from ..base.BaseProcessingStep import BaseProcessingStep

WEBRTC_SAMPLE_RATE = 48000
FRAME_SAMPLES = 960  # 20ms at 48kHz
VIDEO_FPS = 30
DATA_FPS = 20


class FrameSplitterStep(BaseProcessingStep):
    """
    Clock-driven group output for WebRTC streaming.

    Overrides BaseProcessingStep.run() with an absolute-time clock loop.
    Paused until connection_start signal; pauses on connection_stop.

    Each tick outputs exactly one group:
      - Audio group from TTS when available
      - Default silence group when idle

    Signals (SoS, EoS, etc.) are buffered in order with audio groups
    and flushed at tick boundaries to preserve ordering.

    Group size is calculated from GCD of audio/video/data frame rates
    (all configurable via config). Standard output format:
      {"audio": [<pcm>...], "video": [<jpeg>...], "data": [{...}, null, ...]}
    """

    def custom_init(self):
        self.sample_rate = self.get_config("sample_rate", WEBRTC_SAMPLE_RATE)
        self.frame_samples = self.get_config("frame_samples", FRAME_SAMPLES)
        self.video_fps = self.get_config("video_fps", VIDEO_FPS)
        self.data_fps = self.get_config("data_fps", DATA_FPS)
        self.video_width = self.get_config("video_width", 320)
        self.video_height = self.get_config("video_height", 240)

        # Calculate sync group size from GCD of all frame rates
        audio_fps = self.sample_rate // self.frame_samples  # 50
        g = gcd(gcd(audio_fps, self.video_fps), self.data_fps)  # gcd(50,30,20) = 10
        self.audio_per_group = audio_fps // g          # 5
        self.video_per_group = self.video_fps // g     # 3
        self.data_per_group = self.data_fps // g       # 2
        self._group_period = self.audio_per_group * self.frame_samples / self.sample_rate
        group_ms = self._group_period * 1000

        # Pre-generate reusable frames
        self._idle_frame_b64 = self._make_jpeg_b64(173, 216, 230)      # light blue: idle
        self._speaking_frame_b64 = self._make_jpeg_b64(144, 238, 144)  # light green: speaking
        self._silence_audio = [
            base64.b64encode(
                np.zeros(self.frame_samples, dtype=np.int16).tobytes()
            ).decode("ascii")
        ] * self.audio_per_group

        # Internal buffer: only media groups ("group", json_str)
        self._group_buffer = deque()
        self._clock_running = False

        # Stats counters (reset every 100 groups in _run_clock)
        self._stats_silence = 0
        self._stats_content = 0

        # Catch connection_start signal from WebRTC server
        self.catch_signal_set = {"connection_start"}

        self.logger.info(
            f"Sync group: {self.audio_per_group} audio + {self.video_per_group} video "
            f"+ {self.data_per_group} data ({group_ms:.0f}ms), "
            f"video {self.video_width}x{self.video_height}, "
            f"clock-driven output (paused until connection_start)"
        )

    def _make_jpeg_b64(self, r, g, b):
        """Generate a solid color JPEG frame, base64 encoded."""
        img = Image.new("RGB", (self.video_width, self.video_height), (r, g, b))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return base64.b64encode(buf.getvalue()).decode("ascii")

    # ── Clock-driven run loop (overrides BaseProcessingStep.run) ──

    def run(self):
        while True:
            if self.kill_event.is_set():
                self.dispose()
                break

            if not self._clock_running:
                # Paused: wait for input like a normal module
                self._wait_for_start()
                continue

            # Active: run clock-driven output
            self._run_clock()

    def _wait_for_start(self):
        """Block on input_queue waiting for connection_start signal.
        Forwards non-matching messages, handles cancel/kill."""
        try:
            raw = self.input_queue.get(timeout=1)
        except queue.Empty:
            self.check_cancel()
            return

        data = json.loads(raw)
        self.check_cancel()

        # Discard cancelled messages
        ts = data.get("timestamp")
        if ts is not None and ts < self.cancel_timestamp:
            return

        # Forward messages not destined for this node
        dest = data.get("destination", self.index)
        if dest != self.index and dest != -2:
            self.output_queue.put(json.dumps(data))
            return

        signal = data.get("signal", "")
        if signal == "connection_start":
            self._clock_running = True
            self.logger.info("connection_start received, clock started")
            return

        # Forward any other signal or data (shouldn't happen when paused, but be safe)
        if signal and signal != "connection_stop":
            data.pop("destination", None)
            self.output_queue.put(json.dumps(data))

    def custom_cancel(self, cancel_message):
        """Clear buffer when current input is cancelled."""
        self._group_buffer.clear()

    def _run_clock(self):
        """Run the 100ms clock loop until connection_stop or kill."""
        clock_start = time.time()
        group_index = 0

        # Stats
        _si = 100  # log every 100 groups (~10s)
        _max_jitter_ms = 0.0
        self._stats_silence = 0
        self._stats_content = 0

        while self._clock_running:
            if self.kill_event.is_set():
                return

            # Step 1: Cancel check (same as base)
            # current_timestamp is None → no-op
            # current_timestamp set and < cancel_timestamp → custom_cancel clears buffer
            self.check_cancel()

            # Step 2+3: Fill buffer and extract one media group to send.
            # Signals are forwarded inline; if buffer empties after a signal, refill.
            group_to_send = None
            while group_to_send is None:
                if not self._group_buffer:
                    self.current_timestamp = None  # previous input fully sent
                    self._fill_buffer()

                if not self._group_buffer:
                    break  # _fill_default should have filled, safety

                entry_type, data_json = self._group_buffer.popleft()
                if entry_type == "signal":
                    self.output_queue.put(data_json)  # forward signal immediately
                    continue
                group_to_send = data_json

            # Wait for tick (absolute time, no drift)
            next_tick = clock_start + group_index * self._group_period
            remaining = next_tick - time.time()
            if remaining > 0:
                time.sleep(remaining)

            jitter_ms = (time.time() - next_tick) * 1000
            if abs(jitter_ms) > abs(_max_jitter_ms):
                _max_jitter_ms = jitter_ms

            # Step 3: Send the media group
            if group_to_send is not None:
                self.output_queue.put(group_to_send)

            group_index += 1

            if group_index % _si == 0:
                elapsed = time.time() - clock_start
                self.logger.info(
                    f"STATS:splitter t={elapsed:.1f}s idx={group_index} "
                    f"gbuf={len(self._group_buffer)} "
                    f"qout={self.output_queue.qsize()} "
                    f"content={self._stats_content} silence={self._stats_silence} "
                    f"jitter_max={_max_jitter_ms:.1f}ms"
                )
                _max_jitter_ms = 0.0
                self._stats_silence = 0
                self._stats_content = 0

    def _fill_buffer(self):
        """Fill buffer when empty. Standard input flow: read from input_queue,
        discard cancelled, until valid input found or queue empty (fill default)."""
        while not self._group_buffer:
            try:
                raw = self.input_queue.get_nowait()
            except queue.Empty:
                # No input available → fill default
                self._fill_default()
                return

            data = json.loads(raw)
            ts = data.get("timestamp")

            # Destination routing: forward if not for this node
            dest = data.get("destination", self.index)
            if dest != self.index and dest != -2:
                self.output_queue.put(json.dumps(data))
                continue

            # Cancel check: discard old messages
            if ts is not None and ts < self.cancel_timestamp:
                self.logger.info(f"discarding old data: {data}")
                continue

            signal = data.get("signal", "")

            if signal == "connection_start":
                continue

            # Valid signal: add to buffer, set current_timestamp for cancel tracking
            if signal and signal not in self.catch_signal_set:
                data.pop("destination", None)
                self._group_buffer.append(("signal", json.dumps(data)))
                self.current_timestamp = ts
                return

            # Valid audio data: split into groups
            filtered_data = self.extract_input_data(data)
            pass_data = self.extract_pass_data(data)
            self._split_to_buffer(filtered_data, pass_data)
            if self._group_buffer:
                # Buffer was filled — set current_timestamp for cancel tracking
                self.current_timestamp = ts
                return
            # No audio_data in message — continue reading next message

    def _fill_default(self):
        """Fill buffer with default content when no input available.
        Currently fills one silence group. current_timestamp stays None."""
        self._stats_silence += 1
        frame_data = {}
        self.add_output(frame_data, "audio", self._silence_audio)
        self.add_output(frame_data, "video",
                        [self._idle_frame_b64] * self.video_per_group)
        self.add_output(frame_data, "data", [None] * self.data_per_group)
        self.add_destination(frame_data)
        self._group_buffer.append(("group", json.dumps(frame_data)))

    def _split_to_buffer(self, data, pass_data):
        """Split TTS audio into groups and append to internal buffer."""
        audio_data = data.get("audio_data", "")
        if not audio_data:
            return

        pcm_frames = self._decode_and_split(audio_data)
        if not pcm_frames:
            return

        meta = {k: v for k, v in pass_data.items() if v}

        group_count = 0
        for i in range(0, len(pcm_frames), self.audio_per_group):
            group_audio = pcm_frames[i:i + self.audio_per_group]

            # Pad last group if incomplete
            while len(group_audio) < self.audio_per_group:
                group_audio.append(np.zeros(self.frame_samples, dtype=np.int16))

            audio_list = [
                base64.b64encode(f.tobytes()).decode("ascii")
                for f in group_audio
            ]
            video_list = [self._speaking_frame_b64] * self.video_per_group

            data_list = [None] * self.data_per_group
            if group_count == 0 and meta:
                data_list[0] = meta

            frame_data = {}
            self.add_output(frame_data, "audio", audio_list)
            self.add_output(frame_data, "video", video_list)
            self.add_output(frame_data, "data", data_list)
            self.add_destination(frame_data)

            self._group_buffer.append(("group", json.dumps(frame_data)))
            group_count += 1

        self._stats_content += group_count
        duration = len(pcm_frames) * self.frame_samples / self.sample_rate
        self.logger.info(
            f"Buffered {group_count} groups ({duration:.2f}s), "
            f"queue={len(self._group_buffer)}"
        )

    # ── Audio decoding (unchanged) ──

    def _decode_and_split(self, audio_b64):
        """Decode base64 WAV, resample to target rate, split into frames."""
        try:
            wav_bytes = base64.b64decode(audio_b64)
            with wave.open(io.BytesIO(wav_bytes), "rb") as wf:
                sr = wf.getframerate()
                ch = wf.getnchannels()
                raw = wf.readframes(wf.getnframes())
        except Exception as e:
            self.logger.error(f"Failed to decode WAV: {e}")
            return []

        pcm = np.frombuffer(raw, dtype=np.int16)
        if ch > 1:
            pcm = pcm[::ch]

        # Resample if needed
        if sr != self.sample_rate:
            target_len = int(len(pcm) * self.sample_rate / sr)
            pcm = np.interp(
                np.linspace(0, len(pcm) - 1, target_len),
                np.arange(len(pcm)),
                pcm.astype(np.float64),
            ).astype(np.int16)

        # Split into frames
        frames = []
        for i in range(0, len(pcm), self.frame_samples):
            chunk = pcm[i:i + self.frame_samples]
            if len(chunk) < self.frame_samples:
                chunk = np.pad(chunk, (0, self.frame_samples - len(chunk)))
            frames.append(chunk)
        return frames
