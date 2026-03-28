from .BaseProcessingStep import BaseProcessingStep
from .BaseProcessingStep import FuncA
from .BaseProcessingStep import FuncB
from .SpanProcessingStep import SpanProcessingStep

function_map = {
    "default": BaseProcessingStep,
    "call_func_a": FuncA,
    "call_func_b": FuncB,
}
