from .VideoStep import VideoStep, BaseVideoCaller
from .VideoChunkGenerationStep import VideoChunkGenerationStep

function_map = {
    "call_video": VideoStep,
    "call_video_chunk_generation": VideoChunkGenerationStep,
}

# Caller registered like the TTS/motion callers, so the joint stream node can
# resolve it by config name.
caller_map = {
    "video_base": BaseVideoCaller,
}
