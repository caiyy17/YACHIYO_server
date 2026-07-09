from .VideoStep import VideoStep, BaseVideoCaller

function_map = {
    "call_video": VideoStep,
}

# Caller registered like the TTS/motion callers, so the joint stream node can
# resolve it by config name.
caller_map = {
    "video_base": BaseVideoCaller,
}
