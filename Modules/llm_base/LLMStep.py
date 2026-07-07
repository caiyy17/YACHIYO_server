from ..base.BaseProcessingStep import BaseProcessingStep
from ..llm_utils.SimpleHistory import SimpleHistory


class BaseLLMCaller:
    def __init__(self, id, config, logger):
        self.client_id = id
        self.config = config
        self.logger = logger
        self.custom_init()

    def custom_init(self):
        self.logger.info("LLM caller initialized")
        self.history_manager = SimpleHistory(self.client_id, self.config)

    def cancel(self, cancel_message):
        self.history_manager.cancel(cancel_message)

    def call_stream(self, prompt, allow_tools=True):
        try:
            self.history_manager.load_history()
            modified_history = self.history_manager.modify_history(prompt)
            result = self.generate_result(modified_history, allow_tools=allow_tools)
            accumulated_response = []
            for response in result:
                if response is None:
                    yield None
                    continue
                accumulated_response.append(response)
                self.history_manager.extra_info["current_response"] = (
                    accumulated_response
                )
                current_history = self.history_manager.prepare_saving()
                self.history_manager.save_history(current_history)
                yield response
        except Exception as e:
            self.logger.error(f"call_stream error: {e}")
            return "error"

    def generate_result(history):
        response = {}
        response["text"] = history[-1]["content"]
        response["raw_text"] = history[-1]["content"]
        yield response


class LLMStep(BaseProcessingStep):
    REQUIRED_INPUTS = ["prompt"]

    EMIT_SIGNALS = ["SoS", "EoS"]  # broadcast turn envelope

    def custom_init(self):
        self.llm_caller = BaseLLMCaller(self.client_id, self.config, self.logger)

    def custom_cancel(self, cancel_message):
        self.llm_caller.cancel(cancel_message)

    def process(self, data, pass_data={}):
        prompt = data.get("prompt", "")
        self.emit_signal("SoS", pass_data, is_add_destination=False)
        for response in self.llm_caller.call_stream(prompt):
            if self.check_cancel():
                self.logger.info("cancel inside loop")
                break
            if response is None:
                continue
            current_data = {}
            for key, value in response.items():
                self.add_output(current_data, key, value)
            self.output_to_queue(current_data, pass_data)
        self.emit_signal("EoS", pass_data, is_add_destination=False)
        return
