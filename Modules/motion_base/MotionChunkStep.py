from ..base.ChunkGenerationStep import (
    ChunkGenerationSession,
    ChunkGenerationStep,
    frames_for_duration,
)


CHUNK_DURATION_MS = 200


def neutral_motion_frame():
    return {
        "root_dxz": [0.0, 0.0],
        "root_dy": 0.0,
        "root_dyaw": 0.0,
        "hips_pos": [0.0, 0.0, 0.0],
        "joints": {},
    }


class _NeutralMotionSession(ChunkGenerationSession):
    def __init__(self, frames_per_chunk, framerate):
        self.frames_per_chunk = frames_per_chunk
        self.framerate = framerate
        self.first = True

    def generate_chunk(self, inputs, chunk_index):
        frames = [neutral_motion_frame()
                  for _ in range(self.frames_per_chunk)]
        if self.first:
            frames[0] = {
                "header": {
                    "framerate": self.framerate,
                    "format": "humanoid",
                },
                **frames[0],
            }
            self.first = False
        return {"motion": frames}


class MotionChunkStep(ChunkGenerationStep):
    """Base incremental motion generator producing neutral fixed chunks."""

    OUTPUTS = ["motion"]

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        framerate = config.get("framerate", 30)
        if isinstance(framerate, bool) or not isinstance(framerate, int) \
                or framerate <= 0:
            errors.append(
                f"framerate must be an int > 0, got {framerate!r}"
            )
        duration = config.get("chunk_duration_ms", CHUNK_DURATION_MS)
        if isinstance(duration, bool) \
                or not isinstance(duration, (int, float)) or duration <= 0:
            errors.append(
                f"chunk_duration_ms must be a number > 0, got {duration!r}"
            )
        else:
            try:
                frames_for_duration(duration, framerate)
            except (TypeError, ValueError, ZeroDivisionError) as error:
                errors.append(str(error))
        return errors

    def generation_init(self):
        self.framerate = self.get_config("framerate", 30)
        self.frames_per_chunk = frames_for_duration(
            self.get_config("chunk_duration_ms", CHUNK_DURATION_MS),
            self.framerate,
        )

    def open_generation_session(self, start_context):
        return _NeutralMotionSession(
            self.frames_per_chunk, self.framerate
        )
