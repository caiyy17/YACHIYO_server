import base64
import io
import json
import queue
import time
import wave
from collections import deque
from math import gcd

import numpy as np
from PIL import Image, ImageDraw, ImageFont

from ..base.BaseProcessingStep import BaseProcessingStep

AUDIO_SAMPLE_RATE = 48000  # fixed by WebRTC: Opus runs at a 48kHz clock, not configurable
AUDIO_FPS = 50  # 20ms audio frames
VIDEO_FPS = 30
DATA_FPS = 20

# Frame background colors (RGB)
IDLE_COLOR = (173, 216, 230)   # light blue
SPEAK_COLOR = (144, 238, 144)  # light green

FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "DejaVuSans-Bold.ttf",
]


class FrameSplitterStep(BaseProcessingStep):
    REQUIRED_CATCH_SIGNALS = ["connection_start"]
    REQUIRED_INPUTS = ["audio_data"]
    EMIT_SIGNALS = ["meta"]  # pass_vars data shipped to the client

    @classmethod
    def emitted_signals(cls, config):
        # "meta" is only emitted when the node has pass_vars to forward
        return ["meta"] if config.get("pass_vars") else []

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        rates = {
            "audio_fps": config.get("audio_fps", AUDIO_FPS),
            "video_fps": config.get("video_fps", VIDEO_FPS),
            "data_fps": config.get("data_fps", DATA_FPS),
        }
        for name, v in rates.items():
            if not isinstance(v, int) or v <= 0:
                errors.append(f"{name} must be a positive integer, got {v!r}")
                return errors
        if AUDIO_SAMPLE_RATE % rates["audio_fps"] != 0:
            errors.append(
                f"audio_fps {rates['audio_fps']} does not divide the fixed "
                f"{AUDIO_SAMPLE_RATE}Hz WebRTC sample rate — the audio "
                f"frame must be a whole number of samples"
            )
        return errors

    """
    Clock-driven group output for WebRTC streaming.

    Overrides BaseProcessingStep.run() with an absolute-time clock loop.
    Paused until connection_start signal; pauses on connection_stop.

    Each tick outputs exactly one group:
      - Audio group from TTS when available
      - Default silence group when idle

    Signals (SoS, EoS, etc.) are buffered in order with audio groups
    and flushed at tick boundaries to preserve ordering.

    Pass_vars data on an incoming message ships to the client as a "meta"
    signal (wire name from emit_signals config), interleaved in group order
    right before that message's audio groups. The group data lane is
    reserved for frame-aligned payloads.

    Group size is calculated from GCD of audio/video/data frame rates
    (all configurable via config). Standard output format:
      {"audio": [<pcm>...], "video": [<jpeg>...], "data": [{...}, null, ...]}
    """

    def custom_init(self):
        # Per-lane config: <lane>_fps plus the video size; frame sizes and
        # group layout are derived from these. The audio sample rate is
        # fixed at 48kHz by WebRTC and is not a config key.
        self.sample_rate = AUDIO_SAMPLE_RATE
        audio_fps = self.get_config("audio_fps", AUDIO_FPS)
        self.frame_samples = self.sample_rate // audio_fps
        self.video_fps = self.get_config("video_fps", VIDEO_FPS)
        self.data_fps = self.get_config("data_fps", DATA_FPS)
        self.video_width = self.get_config("video_width", 320)
        self.video_height = self.get_config("video_height", 240)

        # Sync group: rate = GCD of the lane rates, so every lane packs a
        # whole number of frames per group
        g = gcd(gcd(audio_fps, self.video_fps), self.data_fps)  # gcd(50,30,20) = 10
        self.audio_per_group = audio_fps // g          # 5
        self.video_per_group = self.video_fps // g     # 3
        self.data_per_group = self.data_fps // g       # 2
        self._group_period = self.audio_per_group * self.frame_samples / self.sample_rate
        group_ms = self._group_period * 1000

        # Lane routing of inputs by target name: "audio_data" -> audio lane,
        # "video_data" -> video lane, every OTHER input -> the data lane as a
        # per-frame list (opaque frames, keyed by its target name). Multiple
        # data-lane inputs (motion, expression, ...) are merged per frame:
        # data slot f = {target: list_f for each data-lane input}.
        self.data_lane_keys = [
            v.get("target") for v in self.config.get("input_vars", [])
            if v.get("target") not in ("audio_data", "video_data")
        ]

        # Pre-create background templates and font for per-frame numbered overlay
        # (frames are rendered with the current counter at SEND time, not generation
        # time, so cancel-clearing the buffer never leaves gaps in the emitted seq.)
        self._idle_base = Image.new("RGB", (self.video_width, self.video_height), IDLE_COLOR)
        self._speak_base = Image.new("RGB", (self.video_width, self.video_height), SPEAK_COLOR)
        font_size = max(40, min(self.video_width, self.video_height) // 8)
        self._font = self._load_font(font_size)
        self._video_frame_counter = 0

        self._silence_audio = [
            base64.b64encode(
                np.zeros(self.frame_samples, dtype=np.int16).tobytes()
            ).decode("ascii")
        ] * self.audio_per_group

        # Internal buffer: ("group", dict-with-video-markers) or ("signal", json_str)
        # Video markers are strings "idle"/"speak"; JPEGs are rendered on pop.
        self._group_buffer = deque()
        self._clock_running = False

        # Stats counters (reset every 100 groups in _run_clock)
        self._stats_silence = 0
        self._stats_content = 0

        # Requires config: catch_signals: ["connection_start"]; signals
        # passing through to the client (SoS/EoS/recording_*) must be
        # declared in pass_signals — they are interleaved in group order.
        self.logger.info(
            f"Sync group: {self.audio_per_group} audio + {self.video_per_group} video "
            f"+ {self.data_per_group} data ({group_ms:.0f}ms), "
            f"video {self.video_width}x{self.video_height}, "
            f"clock-driven output (paused until connection_start)"
        )

    def _load_font(self, size):
        for path in FONT_CANDIDATES:
            try:
                return ImageFont.truetype(path, size)
            except (OSError, IOError):
                continue
        return ImageFont.load_default()

    def _render_jpeg_b64(self, kind, frame_num):
        """Render a numbered frame. kind in {"idle","speak"}.
        ~1-2ms per frame at 1280x720 on a modern CPU; called lazily at send time."""
        base = self._speak_base if kind == "speak" else self._idle_base
        img = base.copy()
        draw = ImageDraw.Draw(img)
        text = f"#{frame_num}"
        # Center the text
        bbox = draw.textbbox((0, 0), text, font=self._font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        x = (self.video_width - tw) // 2 - bbox[0]
        y = (self.video_height - th) // 2 - bbox[1]
        # Black outline (4-direction) + white fill for visibility on any background
        for dx, dy in ((-3, 0), (3, 0), (0, -3), (0, 3)):
            draw.text((x + dx, y + dy), text, fill=(0, 0, 0), font=self._font)
        draw.text((x, y), text, fill=(255, 255, 255), font=self._font)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return base64.b64encode(buf.getvalue()).decode("ascii")

    def _render_group_videos(self, group_dict):
        """Replace "idle"/"speak" markers with numbered JPEG frames, incrementing
        the counter once per frame. Called at pop time so cancel never wastes ids."""
        for target in self.output_dict.get("video", []):
            markers = group_dict.get(target)
            if not isinstance(markers, list):
                continue
            rendered = []
            for marker in markers:
                rendered.append(self._render_jpeg_b64(marker, self._video_frame_counter))
                self._video_frame_counter += 1
            group_dict[target] = rendered

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
        if dest != self.index:
            self.output_queue.put(json.dumps(data))
            return

        signal = data.get("signal", "")
        if signal == "connection_start":
            self._clock_running = True
            self.logger.info("connection_start received, clock started")
            return

        # Paused state: apply the same four-state rules for other signals
        # (one relay copy per declared pass target)
        if signal:
            if signal in self.pass_signal_set:
                self._relay_caught(data, signal)
            else:
                self.logger.error(
                    f"undeclared/uncatchable signal '{signal}' at paused "
                    f"splitter; dropping"
                )

    def custom_cancel(self, cancel_message):
        """Clear buffers when current input is cancelled."""
        self._group_buffer.clear()

    def _run_clock(self):
        """Run the 100ms clock loop until connection_stop or kill."""
        clock_start = time.time()
        group_index = 0
        # Reset frame counter per connection so numbering starts from 0
        self._video_frame_counter = 0

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

                entry_type, entry = self._group_buffer.popleft()
                if entry_type == "signal":
                    self.output_queue.put(entry)  # forward signal immediately (json str)
                    continue
                # Media group: entry is a dict with "idle"/"speak" markers; render now
                self._render_group_videos(entry)
                group_to_send = json.dumps(entry)

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
            if dest != self.index:
                self.output_queue.put(json.dumps(data))
                continue

            # Cancel check: discard old messages
            if ts is not None and ts < self.cancel_timestamp:
                self.logger.info(f"discarding old data: {data}")
                continue

            signal = data.get("signal", "")

            if signal == "connection_start":
                continue  # already running; duplicate start is a no-op

            # Four-state signal rules (see BaseProcessingStep). The splitter
            # has no generic handler beyond connection_start, so a caught
            # signal here is a wiring mistake — interleave it instead of
            # silently dropping it into the content path.
            if signal:
                if signal in self.pass_signal_set:
                    relay = {k: v for k, v in data.items()
                             if k != "destination"}
                    relay["signal"] = self.pass_signal_map[signal]
                    self.add_destination(relay)
                    self._group_buffer.append(("signal", json.dumps(relay)))
                    self.current_timestamp = ts
                elif signal in self.catch_signal_set:
                    self.logger.error(
                        f"splitter cannot process caught signal '{signal}'; "
                        f"interleaving as pass-through"
                    )
                    data.pop("destination", None)
                    self._group_buffer.append(("signal", json.dumps(data)))
                    self.current_timestamp = ts
                else:
                    self.logger.error(
                        f"undeclared signal '{signal}' at splitter; dropping "
                        f"— declare it in catch_signals or pass_signals"
                    )
                return

            # Content message: pass_vars data ships as a "meta" signal,
            # the audio (if any) is split into groups behind it
            filtered_data = self.extract_input_data(data)
            pass_data = self.extract_pass_data(data)
            self._split_to_buffer(filtered_data, pass_data)
            if self._group_buffer:
                # Buffer was filled — set current_timestamp for cancel tracking
                self.current_timestamp = ts
                return
            # nothing shippable in this message: continue reading the next

    def _fill_default(self):
        """Fill buffer with default content when no input available.
        Currently fills one silence group. current_timestamp stays None."""
        self._stats_silence += 1
        frame_data = {}
        self.add_output(frame_data, "audio", self._silence_audio)
        self.add_output(frame_data, "video", ["idle"] * self.video_per_group)
        self.add_output(frame_data, "data", [None] * self.data_per_group)
        self.add_destination(frame_data)
        self._group_buffer.append(("group", frame_data))

    def _buffer_meta_signal(self, pass_data):
        """Ship pass_vars data as a "meta" signal (same shape as SoS:
        timestamp top-level, data wrapped under "pass_data"). Buffered
        instead of emit_signal so it stays in group order and is
        cancel-cleared together with its groups. Empty data ships nothing."""
        wrapped = {k: v for k, v in pass_data.items()
                   if k != "timestamp" and v}
        if not wrapped:
            return
        wire = self.emit_signal_map.get("meta")
        if wire is None:
            self.logger.error(
                "emit_signal('meta') is not declared in emit_signals; "
                "dropping — declare it in the node config"
            )
            return
        msg = {"signal": wire,
               "timestamp": pass_data.get("timestamp"),
               "pass_data": wrapped}
        self.add_destination(msg)
        self._group_buffer.append(("signal", json.dumps(msg)))

    def _split_to_buffer(self, data, pass_data):
        """Buffer one message: its pass_vars data as a "meta" signal, then a
        run of groups. Audio (audio_data) is split into audio frames; every
        data-lane input (any input other than audio_data/video_data — e.g.
        motion, expression) is a per-frame list, merged per frame into the
        data slots. Frames are opaque to the splitter — no schema knowledge.

        The run spans max(audio groups, data groups): a group past the audio
        gets silence audio (so a data-only message — e.g. motion outlasting
        speech — still emits groups), and a data slot past its inputs is None.
        Data slot f = {target: list_f for each data-lane input that has frame f}."""
        self._buffer_meta_signal(pass_data)

        audio_data = data.get("audio_data", "")
        pcm_frames = self._decode_and_split(audio_data) if audio_data else []

        # per-frame lists for the data lane, keyed by input target name
        lane = {k: (data.get(k) or []) for k in self.data_lane_keys}
        max_frames = max((len(v) for v in lane.values()), default=0)

        apg, dpg = self.audio_per_group, self.data_per_group
        n_audio_groups = (len(pcm_frames) + apg - 1) // apg
        n_data_groups = (max_frames + dpg - 1) // dpg
        n_groups = max(n_audio_groups, n_data_groups)
        if n_groups == 0:
            return  # nothing shippable

        for g in range(n_groups):
            group_audio = pcm_frames[g * apg:(g + 1) * apg]
            has_audio = len(group_audio) > 0
            # pad to a full group with silence (also covers groups past the audio)
            while len(group_audio) < apg:
                group_audio.append(np.zeros(self.frame_samples, dtype=np.int16))

            audio_list = [
                base64.b64encode(f.tobytes()).decode("ascii")
                for f in group_audio
            ]
            video_list = [("speak" if has_audio else "idle")] * self.video_per_group

            # data slots: for each frame index, merge the f-th frame of every
            # data-lane input; None if no input has that frame
            data_list = []
            for slot in range(dpg):
                f = g * dpg + slot
                merged = {k: v[f] for k, v in lane.items() if f < len(v)}
                data_list.append(merged or None)

            frame_data = {}
            self.add_output(frame_data, "audio", audio_list)
            self.add_output(frame_data, "video", video_list)
            self.add_output(frame_data, "data", data_list)
            self.add_destination(frame_data)

            self._group_buffer.append(("group", frame_data))

        self._stats_content += n_groups
        adur = len(pcm_frames) * self.frame_samples / self.sample_rate
        self.logger.info(
            f"Buffered {n_groups} groups (audio {adur:.2f}s / {max_frames} "
            f"data frames), queue={len(self._group_buffer)}"
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
