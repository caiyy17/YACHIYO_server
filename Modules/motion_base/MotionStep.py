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
        """Yield chunks as {"motion": <payload>} — every caller's stream
        product is a dict keyed by its product names (one uniform shape).
        Base fallback: a single chunk holding the full call() result; real
        streaming callers override."""
        result = self.call(prompt)
        if result != "":
            yield {"motion": result}

    def reset_history(self):
        pass


class MotionStep(BaseProcessingStep):
    @classmethod
    def required_catch_signals(cls, config):
        # continuous mode consumes SoS to reset continuation history
        return ["SoS"] if config.get("continuous") else []

    REQUIRED_INPUTS = ["prompt"]
    # Sentence-level stream envelope, emitted only in stream mode (see
    # emitted_signals); wire names must be renamed in config when the
    # turn-level SoS/EoS also passes through (clash check enforces it).
    EMIT_SIGNALS = ["SoS", "EoS"]

    @classmethod
    def emitted_signals(cls, config):
        return ["SoS", "EoS"] if config.get("stream") else []

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
        """Single-in-multi-out protocol, same shape as the LLM turn: a
        sentence-level SoS opens the chunk stream and carries the per-
        sentence pass_vars data (wrapped under "pass_data"); every chunk
        message is uniform (payload + timestamp only); a sentence-level EoS
        closes the stream. On cancel the envelope is NOT closed — the whole
        turn is stale anyway."""
        start = {"timestamp": pass_data.get("timestamp")}
        wrapped = {k: v for k, v in pass_data.items() if k != "timestamp"}
        if wrapped:
            start["pass_data"] = wrapped
        self.emit_signal("SoS", start)
        for chunk in self.motion_caller.call_stream(prompt):
            if self.check_cancel():
                self.logger.info("cancelled during motion stream")
                return
            chunk = chunk.get("motion") if isinstance(chunk, dict) else None
            if chunk == "" or chunk is None:
                continue
            output_data = {}
            self.add_output(output_data, "motion", chunk)
            self.output_to_queue(output_data, pass_data,
                                 is_add_pass_data=False)
        self.emit_signal("EoS", {"timestamp": pass_data.get("timestamp")})
        return
