from .DispatcherStep import DispatcherStep
from .ReceiverStep import ReceiverStep

function_map = {
    "call_dispatcher": DispatcherStep,
    "call_receiver": ReceiverStep,
}
