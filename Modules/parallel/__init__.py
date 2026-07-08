from .DispatcherStep import DispatcherStep
from .ReceiverStep import ReceiverStep
from .JointStreamStep import JointStreamStep

function_map = {
    "call_dispatcher": DispatcherStep,
    "call_receiver": ReceiverStep,
    "call_joint_stream": JointStreamStep,
}
