from .OpenaiTTSStep import OpenaiTTSStep, OpenaiTTSCaller

function_map = {
    "call_openai_tts": OpenaiTTSStep,
}

# Callers are registered like steps, so composite modules (e.g. the joint
# stream node) resolve them by config name instead of importing across
# module boundaries.
caller_map = {
    "openai_tts": OpenaiTTSCaller,
}
