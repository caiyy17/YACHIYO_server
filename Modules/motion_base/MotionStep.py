from ..base.BaseProcessingStep import BaseProcessingStep


class BaseMotionCaller:
    """Stub motion caller. Real implementations (e.g. MotionGenerationCaller) take a
    text prompt and return a motion payload; optional continuation via reset_history."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.continuous = False

    def call(self, prompt):
        return {
            "num_frames": 0,
            "framerate": 30,
            "duration": 0.0,
            "root_xz": [],
            "root_vel_y": [],
            "root_vel_yaw": [],
            "hips_pos": [],
            "joints": {},
        }

    def reset_history(self):
        pass


class MotionStep(BaseProcessingStep):
    """Pipeline step: text prompt -> motion. Emits a single ``motion`` output.

    In continuous mode the caller keeps the last N frames as continuation context;
    this step intercepts SoS to reset that history at the start of a stream. All other
    signals (EoS, etc.) pass through automatically.
    """

    def custom_init(self):
        self.motion_caller = BaseMotionCaller(self.config, self.logger)
        if self.motion_caller.continuous:
            self.catch_signal_set = {"SoS"}

    def custom_cancel(self, cancel_message):
        self.motion_caller.reset_history()

    def process(self, data, pass_data={}):
        if data.get("signal", "") == "SoS":
            self.motion_caller.reset_history()
            self.output_to_queue({"signal": "SoS"}, pass_data)
            return

        prompt = data.get("prompt", "")
        result = self.motion_caller.call(prompt) if prompt != "" else ""
        output_data = {}
        self.add_output(output_data, "motion", result)
        self.output_to_queue(output_data, pass_data)
        return
