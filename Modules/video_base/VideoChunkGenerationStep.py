from ..base.ChunkGenerationStep import (
    ChunkGenerationSession,
    ChunkGenerationStep,
    frames_for_duration,
)
from .VideoStep import DEFAULT_COLOR, _solid_frame_b64


CHUNK_DURATION_MS = 200


class _SolidVideoSession(ChunkGenerationSession):
    def __init__(self, image, frames_per_chunk, framerate):
        self.image = image
        self.frames_per_chunk = frames_per_chunk
        self.framerate = framerate
        self.first = True

    def generate_chunk(self, inputs, chunk_index):
        frames = [
            {"image": self.image} for _ in range(self.frames_per_chunk)
        ]
        if self.first:
            frames[0] = {
                "header": {"framerate": self.framerate},
                **frames[0],
            }
            self.first = False
        return {"video": frames}


class VideoChunkGenerationStep(ChunkGenerationStep):
    """Placeholder incremental video generator: one active-color block in,
    one fixed-duration solid-color video block out."""

    OUTPUTS = ["video"]

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        for key, default in (
                ("video_width", 320),
                ("video_height", 240),
                ("video_fps", 30)):
            value = config.get(key, default)
            if isinstance(value, bool) or not isinstance(value, int) \
                    or value <= 0:
                errors.append(f"{key} must be an int > 0, got {value!r}")

        duration = config.get("chunk_duration_ms", CHUNK_DURATION_MS)
        if isinstance(duration, bool) \
                or not isinstance(duration, (int, float)) or duration <= 0:
            errors.append(
                f"chunk_duration_ms must be a number > 0, got {duration!r}"
            )
        else:
            try:
                frames_for_duration(duration, config.get("video_fps", 30))
            except (TypeError, ValueError, ZeroDivisionError) as error:
                errors.append(str(error))

        color = config.get("color", DEFAULT_COLOR)
        if (not isinstance(color, (list, tuple)) or len(color) != 3
                or any(isinstance(value, bool)
                       or not isinstance(value, int)
                       or not 0 <= value <= 255 for value in color)):
            errors.append(
                f"color must be an RGB triple of 0..255 integers, "
                f"got {color!r}"
            )
        return errors

    def generation_init(self):
        self.width = self.get_config("video_width", 320)
        self.height = self.get_config("video_height", 240)
        self.framerate = self.get_config("video_fps", 30)
        self.frames_per_chunk = frames_for_duration(
            self.get_config("chunk_duration_ms", CHUNK_DURATION_MS),
            self.framerate,
        )
        self.image = _solid_frame_b64(
            self.width,
            self.height,
            self.get_config("color", DEFAULT_COLOR),
        )

    def open_generation_session(self, start_context):
        return _SolidVideoSession(
            self.image, self.frames_per_chunk, self.framerate
        )
