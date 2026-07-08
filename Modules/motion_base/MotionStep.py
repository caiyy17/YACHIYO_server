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

    def call_stream(self, prompt):
        """Yield motion chunks. Base fallback: a single chunk holding the full
        call() result, so stream mode works (degenerately) with any caller;
        real streaming callers override this."""
        result = self.call(prompt)
        if result != "":
            yield result

    def reset_history(self):
        pass


class MotionStep(BaseProcessingStep):
    @classmethod
    def required_catch_signals(cls, config):
        # continuous mode consumes SoS to reset continuation history
        return ["SoS"] if config.get("continuous") else []

    # process() understands SoS in any mode (reset is a no-op without history)
    KNOWN_CATCH_SIGNALS = ["SoS"]
    REQUIRED_INPUTS = ["prompt"]

    """Pipeline step: text prompt -> motion. Emits a single ``motion`` output.

    In continuous mode the caller keeps the last N frames as continuation context;
    this step intercepts SoS to reset that history at the start of a stream. All other
    signals (EoS, etc.) pass through automatically.
    """

    def custom_init(self):
        # In continuous mode this node needs config:
        #   catch_signals: ["SoS"], pass_signals: ["SoS"]
        # (reset continuation history on SoS; framework relays it downstream)
        self.motion_caller = BaseMotionCaller(self.config, self.logger)

    def custom_cancel(self, cancel_message):
        self.motion_caller.reset_history()

    def process(self, data, pass_data={}):
        if data.get("signal", "") == "SoS":
            self.motion_caller.reset_history()
            return

        prompt = data.get("prompt", "")

        # stream: true -> one message per motion chunk as the caller produces
        # them (config option; default off keeps the original single-message
        # behavior untouched). Empty prompts keep the original single empty
        # message on both paths.
        if prompt != "" and self.get_config("stream", False):
            self._process_stream(prompt, pass_data)
            return

        result = self.motion_caller.call(prompt) if prompt != "" else ""
        output_data = {}
        self.add_output(output_data, "motion", result)
        self.output_to_queue(output_data, pass_data)
        return

    def _process_stream(self, prompt, pass_data):
        """Emit one message per chunk. The first chunk carries the full
        pass_vars meta (downstream sees it exactly once, same as non-stream);
        later chunks carry only the timestamp, so cancel semantics still
        apply to every chunk."""
        first = True
        for chunk in self.motion_caller.call_stream(prompt):
            if self.check_cancel():
                self.logger.info("cancelled during motion stream")
                return
            if chunk == "" or chunk is None:
                continue
            output_data = {}
            self.add_output(output_data, "motion", chunk)
            if first:
                self.output_to_queue(output_data, pass_data)
                first = False
            else:
                self.output_to_queue(output_data, pass_data, is_add_pass_data=False)
        return
