import base64
import io
import wave

from ..base.BaseProcessingStep import BaseProcessingStep

SAMPLE_RATE = 48000  # fixed by WebRTC: Opus runs at a 48kHz clock, not configurable

# lane input target <-> media output source (the splitter's input names)
MEDIA_PAIRS = (("audio", "audio_data"), ("video", "video_data"))


class FrameCollectorStep(BaseProcessingStep):
    """Per-group transformer, the input-side mirror of FrameSplitterStep.

    Consumes gateway groups ({"audio": [...], "video": [...], "data": [...]})
    and re-emits each lane in the splitter's content shape, one message per
    group, stateless:
      - audio -> "audio_data": the group's PCM frames joined into one WAV
      - video -> "video_data": the group's JPEG frames as a list
      - data  -> one output per demux key: per-slot list of slot[key]
    Output names match the splitter's inputs, so collector -> splitter is a
    valid loopback pipeline. Signals pass through (no catches); recording
    segmentation lives downstream (vad module), so there is no span state
    and cancel needs no custom handling.

    Config contract (validated at init):
      - all three lane inputs (audio / video / data) are declared; a lane
        is WIRED when its source is non-null, and at least one must be
      - lanes are wired on BOTH sides: "audio" input <-> "audio_data"
        output, "video" input <-> "video_data" output, "data" input <-> at
        least one demux-key output (any source name other than the reserved
        audio_data / video_data); a null on either side counts as absent
    """

    REQUIRED_INPUTS = ["audio", "video", "data"]  # null source = lane off
    OUTPUTS = ["audio_data", "video_data"]
    FREE_OUTPUTS = True  # demux keys are config-defined output sources

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        # wired = declared with a non-null wire side (null = lane off)
        in_wired = {v.get("target") for v in config.get("input_vars", [])
                    if v.get("source") is not None}
        out_wired = {v.get("source") for v in config.get("output_vars", [])
                     if v.get("target") is not None}

        if not in_wired & {"audio", "video", "data"}:
            errors.append(
                "at least one lane input (audio / video / data) must be "
                "wired (declared with a non-null source)"
            )
        for lane, out in MEDIA_PAIRS:
            if (lane in in_wired) != (out in out_wired):
                errors.append(
                    f"lane '{lane}' must be wired on both sides: input "
                    f"target '{lane}' <-> output source '{out}' "
                    f"(null counts as absent)"
                )
        demux_keys = out_wired - {out for _, out in MEDIA_PAIRS}
        if ("data" in in_wired) != bool(demux_keys):
            errors.append(
                "lane 'data' must be wired on both sides: input target "
                "'data' <-> at least one demux-key output (a source other "
                "than the reserved audio_data / video_data; null counts "
                "as absent)"
            )
        return errors

    def custom_init(self):
        out_wired = {v.get("source")
                     for v in self.config.get("output_vars", [])
                     if v.get("target") is not None}
        self.demux_keys = sorted(
            out_wired - {out for _, out in MEDIA_PAIRS})
        self.logger.info(
            f"frame collector: lanes audio={'audio' in self.input_targets()} "
            f"video={'video' in self.input_targets()} "
            f"data keys={self.demux_keys}"
        )

    def input_targets(self):
        """Wired lane inputs (null-source declarations are lane off)."""
        return {v.get("target") for v in self.config.get("input_vars", [])
                if v.get("source") is not None}

    def process(self, data, pass_data={}):
        output_data = {}

        frames = data.get("audio")
        if frames:
            self.add_output(output_data, "audio_data",
                            self._frames_to_wav(frames))

        frames = data.get("video")
        if frames:
            self.add_output(output_data, "video_data", frames)

        slots = data.get("data")
        if isinstance(slots, list):
            for key in self.demux_keys:
                self.add_output(output_data, key, [
                    slot.get(key) if isinstance(slot, dict) else None
                    for slot in slots
                ])

        if output_data:
            # one message per group (10/s) — never log the payload
            self.output_to_queue(output_data, pass_data, log_level=0)

    @staticmethod
    def _frames_to_wav(frames):
        """Join the group's base64 PCM frames into one base64 WAV."""
        pcm = b"".join(base64.b64decode(f) for f in frames)
        bio = io.BytesIO()
        with wave.open(bio, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(SAMPLE_RATE)
            wf.writeframes(pcm)
        return base64.b64encode(bio.getvalue()).decode("ascii")
