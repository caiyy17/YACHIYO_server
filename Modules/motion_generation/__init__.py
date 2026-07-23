from .MotionGenerationStep import MotionGenerationStep, MotionGenerationCaller
from .MotionChunkGenerationStep import (
    MotionChunkGenerationStep,
)

function_map = {
    "call_motion_generation": MotionGenerationStep,
    "call_motion_chunk_generation": MotionChunkGenerationStep,
}

caller_map = {
    "motion_generation": MotionGenerationCaller,
}
