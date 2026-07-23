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
# Supported video/data lane rates: the divisors of the 90kHz RTP clock in
# 10..60 (exact integer video PTS steps at the gateway, which enforces the
# same constant; the data lane shares the list to keep the group GCD in
# the same family)
SUPPORTED_LANE_FPS = (10, 12, 15, 16, 18, 20, 24, 25, 30, 36, 40, 45, 48,
                      50, 60)

# Frame background colors (RGB)
IDLE_COLOR = (0, 0, 255)     # pure blue
ACTIVE_COLOR = (0, 255, 0)   # pure green

FONT_CANDIDATES = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "DejaVuSans-Bold.ttf",
]


class FrameSplitterStep(BaseProcessingStep):
    """
    Clock-driven group output for WebRTC streaming.

    Overrides BaseProcessingStep.run() with an absolute-time clock loop.
    Paused until the connection_start event (control plane, caught via
    catch_events); runs until the kill verb.

    Each tick outputs exactly one group:
      - Content groups from input messages when available
      - Default group (silence + idle-blue video) when idle

    Lanes of a content message: audio_data (one WAV, split into PCM frames),
    video_data (a frame list; slots past it repeat the message's last frame;
    a message with no video gets green placeholders), every other input is
    a data-lane per-frame list. A content group's missing audio is silence.

    Signals (SoS, EoS, etc.) are buffered in order with the groups
    and flushed at tick boundaries to preserve ordering.

    Pass_vars data on an incoming message ships to the client as a "meta"
    signal (wire name from emit_signals config), interleaved in group order
    right before that message's audio groups. The group data lane is
    reserved for frame-aligned payloads.

    add_frame_index (config, default off) overlays the running frame number
    on every outgoing video frame — placeholders and real frames alike.

    Group size is calculated from GCD of audio/video/data frame rates
    (all configurable via config). Standard output format:
      {"audio": [<pcm>...], "video": [<jpeg>...], "data": [{...}, null, ...]}
    """

    REQUIRED_CATCH_EVENTS = ["connection_start"]
    # media lanes are fixed contract inputs (null source = lane off); the
    # data lane is free-form (any extra input target becomes a data key).
    # At least one wired input overall, validated below.
    REQUIRED_INPUTS = ["audio_data", "video_data"]
    FREE_INPUTS = True
    OUTPUTS = ["audio", "video", "data"]
    EMIT_SIGNALS = ["meta"]  # pass_vars data shipped to the client

    @classmethod
    def emitted_signals(cls, config):
        # "meta" is only emitted when the node has pass_vars to forward
        return ["meta"] if config.get("pass_vars") else []

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        wired = [v for v in config.get("input_vars", [])
                 if v.get("source") is not None]
        if not wired:
            errors.append(
                "at least one input must be wired (non-null source): "
                "audio_data, video_data, or a data-lane key"
            )
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
        for lane in ("video_fps", "data_fps"):
            if rates[lane] not in SUPPORTED_LANE_FPS:
                errors.append(
                    f"{lane} must be one of {list(SUPPORTED_LANE_FPS)} "
                    f"(divisors of the 90kHz RTP clock in 10..60), got "
                    f"{rates[lane]}"
                )
        return errors

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
            and v.get("source") is not None  # null source = lane off
        ]

        # Pre-create background templates and font. Frames are rendered at
        # SEND time (not generation time) with the current counter, so
        # cancel-clearing the buffer never leaves gaps in the emitted seq.
        self.add_frame_index = self.get_config("add_frame_index", False)
        self._idle_base = Image.new("RGB", (self.video_width, self.video_height), IDLE_COLOR)
        self._active_base = Image.new("RGB", (self.video_width, self.video_height), ACTIVE_COLOR)
        self._fonts = {}  # size -> font; sized per drawn image, cached
        self._video_frame_counter = 0
        self._marker_jpeg_cache = {}  # plain (un-numbered) placeholder JPEGs

        self._silence_audio = [
            base64.b64encode(
                np.zeros(self.frame_samples, dtype=np.int16).tobytes()
            ).decode("ascii")
        ] * self.audio_per_group

        # Internal buffer: ("group", dict) or ("signal", json_str). Video
        # slots hold real b64 JPEG frames and/or "idle"/"active" markers;
        # markers are rendered to placeholder JPEGs on pop.
        self._group_buffer = deque()
        self._clock_running = False

        # Stats counters (reset every 100 groups in _run_clock)
        self._stats_silence = 0
        self._stats_content = 0

        # Requires config: catch_events: ["connection_start"]; signals
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

    def _font_for(self, img):
        """Index font sized to the image being drawn on (1/8 of its short
        side, floor 40) — input frames may differ from the configured
        resolution. Cached per size."""
        size = max(40, min(img.width, img.height) // 8)
        font = self._fonts.get(size)
        if font is None:
            font = self._load_font(size)
            self._fonts[size] = font
        return font

    def _draw_index(self, img, frame_num):
        """Overlay the frame number, centered on the actual image, black
        outline + white fill."""
        font = self._font_for(img)
        draw = ImageDraw.Draw(img)
        text = f"#{frame_num}"
        bbox = draw.textbbox((0, 0), text, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        x = (img.width - tw) // 2 - bbox[0]
        y = (img.height - th) // 2 - bbox[1]
        for dx, dy in ((-3, 0), (3, 0), (0, -3), (0, 3)):
            draw.text((x + dx, y + dy), text, fill=(0, 0, 0), font=font)
        draw.text((x, y), text, fill=(255, 255, 255), font=font)

    @staticmethod
    def _encode_jpeg_b64(img):
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return base64.b64encode(buf.getvalue()).decode("ascii")

    def _render_placeholder(self, kind, frame_num):
        """Placeholder JPEG for a marker ("idle" = blue, "active" = green).
        Numbered when add_frame_index; plain ones are rendered once and
        cached (~1-2ms per numbered frame at 1280x720, at send time)."""
        if self.add_frame_index:
            img = (self._active_base if kind == "active"
                   else self._idle_base).copy()
            self._draw_index(img, frame_num)
            return self._encode_jpeg_b64(img)
        cached = self._marker_jpeg_cache.get(kind)
        if cached is None:
            cached = self._encode_jpeg_b64(
                self._active_base if kind == "active" else self._idle_base)
            self._marker_jpeg_cache[kind] = cached
        return cached

    def _number_real_frame(self, jpeg_b64, frame_num):
        """add_frame_index on a real input frame: decode -> draw -> re-encode
        (~1-2ms per 720p frame — enable the flag only when debugging)."""
        img = Image.open(
            io.BytesIO(base64.b64decode(jpeg_b64))).convert("RGB")
        self._draw_index(img, frame_num)
        return self._encode_jpeg_b64(img)

    def _render_group_videos(self, group_dict):
        """Resolve a group's video slots at pop time: markers become
        placeholder JPEGs, real frames pass through (numbered when
        add_frame_index). The counter advances once per frame at SEND time,
        so cancel-clearing the buffer never leaves gaps in the emitted seq."""
        for target in self.output_dict.get("video", []):
            items = group_dict.get(target)
            if not isinstance(items, list):
                continue
            rendered = []
            for item in items:
                if item in ("idle", "active"):
                    rendered.append(self._render_placeholder(
                        item, self._video_frame_counter))
                elif self.add_frame_index:
                    rendered.append(self._number_real_frame(
                        item, self._video_frame_counter))
                else:
                    rendered.append(item)
                self._video_frame_counter += 1
            group_dict[target] = rendered

    # ── Clock-driven run loop (overrides BaseProcessingStep.run) ──

    def run(self):
        while not self._killed:
            try:
                # _killed is set by check_cancel (called here, in the paused
                # wait, and every clock tick) when a kill verb arrives
                self.check_cancel()
                if self._killed:
                    break

                if not self._clock_running:
                    # Paused: wait for input like a normal module
                    self._wait_for_start()
                    continue

                # Active: run clock-driven output
                self._run_clock()
            except Exception as e:
                self.logger.error(
                    f"splitter run iteration failed; dropped current input: "
                    f"{type(e).__name__}: {e}"
                )
                self._group_buffer.clear()
                self.current_timestamp = None
        try:
            self.dispose()
        except Exception as e:
            self.logger.error(
                f"dispose failed: {type(e).__name__}: {e}"
            )

    def custom_event(self, event):
        """connection_start (control plane): start the output clock. A
        duplicate while running is a no-op."""
        if event.get("signal") != "connection_start":
            return
        if not self._clock_running:
            self._clock_running = True
            self.logger.info("connection_start received, clock started")

    def _wait_for_start(self):
        """Paused state: drain the input queue (forward, relay, discard)
        while waiting for the connection_start event, which arrives on the
        control queue (check_cancel -> custom_event). The short timeout
        bounds the clock-start latency to one poll interval."""
        try:
            raw = self.input_queue.get(timeout=0.05)
        except queue.Empty:
            self.check_cancel()
            return

        data = json.loads(raw)
        self.check_cancel()

        # Forward messages not destined for this node — unconditionally:
        # the cancel gate belongs to the consuming node only
        dest = data.get("destination", self.index)
        if dest != self.index:
            self.output_queue.put(json.dumps(data))
            return

        # Discard cancelled messages (consumed here)
        ts = data.get("timestamp")
        if ts is not None and ts < self.cancel_timestamp:
            return

        # Paused state: apply the same four-state rules for signals
        # (one relay copy per declared pass target)
        signal = data.get("signal", "")
        if signal:
            if signal in self.pass_signal_set:
                self._relay_caught(data, signal)
            else:
                self.logger.warning(
                    f"undeclared signal '{signal}' at paused splitter; "
                    f"dropped"
                )

    def custom_cancel(self, cancel_message):
        """Clear buffers when current input is cancelled."""
        self._group_buffer.clear()

    def _run_clock(self):
        """Run the fixed-period clock loop until the kill verb."""
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
            frame_counter_before = self._video_frame_counter
            content_before = self._stats_content
            silence_before = self._stats_silence
            try:
                # Step 1: Control check (same as base)
                # cancel: current_timestamp set and < cancel_timestamp →
                #         custom_cancel clears buffer
                # kill:   sets _killed → stop the clock; the outer run loop
                #         disposes and exits
                self.check_cancel()
                if self._killed:
                    return

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
                    # Media group: entry is a dict with "idle"/"active" markers; render now
                    self._render_group_videos(entry)
                    group_to_send = json.dumps(entry)

                # Step 3: Send the media group as soon as it is ready. Timing
                # variation is absorbed by the gateway's output buffer.
                if group_to_send is not None:
                    self.output_queue.put(group_to_send)
            except Exception as e:
                self.logger.error(
                    f"splitter tick failed; sending default group: "
                    f"{type(e).__name__}: {e}"
                )
                # The buffered entries belong to the current input. Discard
                # them together, and restore numbering because no media group
                # from this tick was sent.
                self._group_buffer.clear()
                self.current_timestamp = None
                self._video_frame_counter = frame_counter_before
                self._stats_content = content_before
                self._stats_silence = silence_before
                if self._killed:
                    return
                try:
                    self._fill_default()
                    _, fallback = self._group_buffer.popleft()
                    self._render_group_videos(fallback)
                    self.output_queue.put(json.dumps(fallback))
                except Exception as fallback_error:
                    self.logger.error(
                        f"splitter default group failed: "
                        f"{type(fallback_error).__name__}: {fallback_error}"
                    )
                    self._group_buffer.clear()
                    self.current_timestamp = None
                    self._video_frame_counter = frame_counter_before
                    self._stats_content = content_before
                    self._stats_silence = silence_before

            group_index += 1

            # Wait for the start of the next group (absolute time, no drift).
            # Processing therefore starts on the tick; it is never prefetched
            # and held locally while waiting for its send time.
            try:
                next_tick = clock_start + group_index * self._group_period
                remaining = next_tick - time.time()
                if remaining > 0:
                    time.sleep(remaining)

                jitter_ms = (time.time() - next_tick) * 1000
                if abs(jitter_ms) > abs(_max_jitter_ms):
                    _max_jitter_ms = jitter_ms

                if group_index % _si == 0:
                    elapsed = time.time() - clock_start
                    self.logger.info(
                        f"STATS:splitter t={elapsed:.1f}s idx={group_index} "
                        f"gbuf={len(self._group_buffer)} "
                        f"qout={self.output_queue.qsize()} "
                        f"content={self._stats_content} "
                        f"silence={self._stats_silence} "
                        f"jitter_max={_max_jitter_ms:.1f}ms"
                    )
                    _max_jitter_ms = 0.0
                    self._stats_silence = 0
                    self._stats_content = 0
            except Exception as e:
                self.logger.error(
                    f"splitter clock update failed: "
                    f"{type(e).__name__}: {e}"
                )

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

            # Signal rules (the splitter catches no in-band signals — its
            # one control input, connection_start, is a catch_events verb):
            # pass = relay in group order, undeclared = warn + drop.
            if signal:
                if signal in self.pass_signal_set:
                    relay = {k: v for k, v in data.items()
                             if k != "destination"}
                    relay["signal"] = self.pass_signal_map[signal]
                    self.add_destination(relay)
                    self._group_buffer.append(("signal", json.dumps(relay)))
                    self.current_timestamp = ts
                else:
                    self.logger.warning(
                        f"undeclared signal '{signal}' at splitter; dropped "
                        f"(declare it in pass_signals to forward)"
                    )
                    # Dropping an in-band signal must not consume this
                    # clock tick. Keep scanning; if no media follows,
                    # queue.Empty below supplies the normal default group.
                    continue
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
        msg = self.envelope(self.stamp({"signal": wire}, pass_data), wrapped)
        self.add_destination(msg)
        self._group_buffer.append(("signal", json.dumps(msg)))

    def _split_to_buffer(self, data, pass_data):
        """Buffer one message: its pass_vars data as a "meta" signal, then a
        run of groups. Audio (audio_data) is split into audio frames;
        video_data is a frame list; every other input (e.g. motion,
        expression) is a data-lane per-frame list, merged per frame into
        the data slots. Frames are opaque to the splitter — no schema
        knowledge.

        The run spans max(audio, video, data groups): a group past the audio
        gets silence, video slots past the input repeat the message's last
        frame (green placeholders when the message has no video), and a data
        slot past its inputs is None.
        Data slot f = {target: list_f for each data-lane input that has frame f}."""
        self._buffer_meta_signal(pass_data)

        audio_data = data.get("audio_data", "")
        pcm_frames = self._decode_and_split(audio_data) if audio_data else []

        # video frames arrive as {"image": b64} dicts (video product; the
        # first frame may carry extra info like framerate — the splitter
        # paces by its own config, so it is dropped here) or as bare b64
        # strings (webrtc echo path via frame_collector)
        video_frames = [f.get("image", "") if isinstance(f, dict) else f
                        for f in (data.get("video_data") or [])]

        # per-frame lists for the data lane, keyed by input target name
        lane = {k: (data.get(k) or []) for k in self.data_lane_keys}
        max_frames = max((len(v) for v in lane.values()), default=0)

        apg, vpg, dpg = (self.audio_per_group, self.video_per_group,
                         self.data_per_group)
        n_audio_groups = (len(pcm_frames) + apg - 1) // apg
        n_video_groups = (len(video_frames) + vpg - 1) // vpg
        n_data_groups = (max_frames + dpg - 1) // dpg
        n_groups = max(n_audio_groups, n_video_groups, n_data_groups)
        if n_groups == 0:
            return  # nothing shippable

        for g in range(n_groups):
            group_audio = pcm_frames[g * apg:(g + 1) * apg]
            # pad to a full group with silence (also covers groups past the audio)
            while len(group_audio) < apg:
                group_audio.append(np.zeros(self.frame_samples, dtype=np.int16))

            audio_list = [
                base64.b64encode(f.tobytes()).decode("ascii")
                for f in group_audio
            ]

            # video: real frames while the message has them, then repeat the
            # message's last frame; a message with no video at all gets
            # "active" (green) placeholders
            if video_frames:
                video_list = video_frames[g * vpg:(g + 1) * vpg]
                while len(video_list) < vpg:
                    video_list.append(video_frames[-1])
            else:
                video_list = ["active"] * vpg

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

    # ── Audio decoding ──

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
