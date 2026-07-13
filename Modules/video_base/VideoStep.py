import base64
import io

from PIL import Image

from ..base.BaseProcessingStep import BaseProcessingStep

DEFAULT_COLOR = (144, 238, 144)  # light green


def _solid_frame_b64(width, height, color):
    """A single solid-color JPEG frame, base64-encoded."""
    img = Image.new("RGB", (width, height), tuple(color))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return base64.b64encode(buf.getvalue()).decode("ascii")


class BaseVideoCaller:
    """Placeholder video generator — emits a fixed solid-color frame
    (config "color": RGB triple, default green). Mirrors the
    TTS caller: call() returns the whole clip, call_stream() yields chunks.
    A video product is a per-frame list (like motion); each frame is a dict
    {"image": <b64 jpeg>}, and the FIRST frame of a clip/stream additionally
    carries a "header" dict (framerate, plus duration for whole clips —
    same first-frame-header contract as motion). A chunk is a list of
    frames; the uniform caller contract wraps it as {"video": [...]}.
    Config: video_width/video_height (frame size), video_fps, stream_frames
    (frames per chunk), duration (fallback clip length in seconds). Duration
    is normally an INPUT (so the clip length can be driven dynamically, e.g.
    to match the audio); the config value is only the fallback."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.width = int(config.get("video_width", 320))
        self.height = int(config.get("video_height", 240))
        self.fps = int(config.get("video_fps", 30))
        self.default_duration = float(config.get("duration", 2.0))
        self.color = tuple(config.get("color", DEFAULT_COLOR))
        self._frame = _solid_frame_b64(self.width, self.height, self.color)

    def _total_frames(self, duration):
        d = self.default_duration if duration is None else float(duration)
        return max(0, int(self.fps * d))

    def call(self, prompt, duration=None):
        """Non-stream: the whole clip as one per-frame list of green frames;
        the first frame additionally carries a "header" with framerate and
        duration (the streaming path omits duration — unknown upfront)."""
        frames = [{"image": self._frame}
                  for _ in range(self._total_frames(duration))]
        if frames:
            frames[0] = {"header": {"framerate": self.fps,
                                    "duration": len(frames) / self.fps},
                         **frames[0]}
        return frames

    def call_stream(self, prompt, duration=None):
        """Yield chunks as {"video": [frame, ...]} — each chunk is
        `stream_frames` green frames (the last may be shorter); the stream's
        very first frame additionally carries a "header" with framerate."""
        total = self._total_frames(duration)
        chunk = max(1, int(self.config.get("stream_frames", self.fps)))
        first = True
        for i in range(0, total, chunk):
            n = min(chunk, total - i)
            frames = [{"image": self._frame} for _ in range(n)]
            if first and frames:
                frames[0] = {"header": {"framerate": self.fps}, **frames[0]}
                first = False
            yield {"video": frames}


class VideoStep(BaseProcessingStep):
    # duration input = reference length (null: module default); duration
    # OUTPUT = actual clip length from the first-frame header (non-stream
    # only, like TTS/motion)
    REQUIRED_INPUTS = ["prompt", "duration"]
    OUTPUTS = ["video", "duration"]
    # Sentence-level stream envelope, emitted only in stream mode (same as
    # TTS/Motion; wire names renamed in config when a turn-level SoS/EoS also
    # passes through this node).
    EMIT_SIGNALS = ["SoS", "EoS"]

    @classmethod
    def emitted_signals(cls, config):
        return ["SoS", "EoS"] if config.get("stream") else []

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        color = config.get("color", DEFAULT_COLOR)
        if (not isinstance(color, (list, tuple)) or len(color) != 3
                or any(isinstance(v, bool) or not isinstance(v, int)
                       or not 0 <= v <= 255 for v in color)):
            errors.append(
                f"color must be an RGB triple of 0..255 integers, "
                f"got {color!r}")
        return errors

    def custom_init(self):
        self.video_caller = BaseVideoCaller(self.config, self.logger)

    def process(self, data, pass_data={}):
        prompt = data.get("prompt", "")
        # clip length is an optional input; falls back to the config default
        duration = data.get("duration")

        # stream: true -> one message per video chunk (config option; default
        # off keeps a single whole-clip message).
        if self.get_config("stream", False):
            self._process_stream(prompt, duration, pass_data)
            return

        result = self.video_caller.call(prompt, duration)
        output_data = {}
        self.add_output(output_data, "video", result)
        self.add_output(output_data, "duration", self._clip_duration(result))
        self.output_to_queue(output_data, pass_data)
        return

    @staticmethod
    def _clip_duration(frames):
        """Actual clip length from the first frame's header (0.0 when the
        clip is empty or has no header)."""
        if isinstance(frames, list) and frames and isinstance(frames[0], dict):
            return float(frames[0].get("header", {}).get("duration", 0.0))
        return 0.0

    def _process_stream(self, prompt, duration, pass_data):
        """Single-in-multi-out, same envelope as TTS/Motion: sentence-level
        SoS (carrying pass_vars under "pass_data") -> uniform chunk messages
        -> sentence-level EoS. Cancel does not close the envelope."""
        start = {"timestamp": pass_data.get("timestamp")}
        wrapped = {k: v for k, v in pass_data.items() if k != "timestamp"}
        if wrapped:
            start["pass_data"] = wrapped
        self.emit_signal("SoS", start)
        for chunk in self.video_caller.call_stream(prompt, duration):
            if self.check_cancel():
                self.logger.info("cancelled during video stream")
                return
            chunk = chunk.get("video") if isinstance(chunk, dict) else None
            if not chunk:
                continue
            output_data = {}
            self.add_output(output_data, "video", chunk)
            # stream chunks carry b64 frames — never log the payload
            self.output_to_queue(output_data, pass_data,
                                 is_add_pass_data=False, log_level=0)
        self.emit_signal("EoS", {"timestamp": pass_data.get("timestamp")})
        return
