import json

from ..llm_base.LLMStep import SimpleHistory, BaseLLMCaller, LLMStep
from ..llm_utils.TavernHistory import TavernHistory
from ..llm_utils.StreamCutter import StreamCutter
from ..llm_utils.ToolsCaller import ToolsCaller


class OpenaiCaller(BaseLLMCaller):
    def custom_init(self):
        self.history_mode = self.config.get("history_mode", "simple")
        if self.history_mode == "simple":
            self.history_manager = SimpleHistory(self.client_id, self.config)
        elif self.history_mode == "tavern":
            self.history_manager = TavernHistory(
                self.client_id, self.config, self.logger
            )
        else:
            self.history_manager = SimpleHistory(self.client_id, self.config)
        self.client = self.create_client()
        self.cutter = StreamCutter(self.config)
        self.toolsCaller = ToolsCaller(self.config, self.logger)

    def cancel(self, cancel_message):
        super().cancel(cancel_message)
        self.cutter.reset()

    def create_client(self):
        # Load server configurations in "configs/llm/{model_name}.json"
        model_name = self.config.get("model", "gpt")
        with open(f"configs/llm/{model_name}.json", "r") as f:
            self.model_config = json.load(f)
        self.logger.info(f"Model Config: {self.model_config}")

        from openai import OpenAI
        from utils.settings import get_setting, get_secret

        api_base = self.model_config.get("api_base", "")
        api_key = self.model_config.get("api_key", "")
        if api_base == "":
            api_base = None
        else:
            api_base = get_setting("llm", api_base)

        if api_key == "":
            api_key = "EMPTY"
        else:
            api_key = get_secret(api_key)

        client = OpenAI(api_key=api_key, base_url=api_base)
        self.model = self.config.get("model", "gpt-4o")
        self._init_call(client)
        return client

    def _init_call(self, client):
        """Init call with full system prompt to warm up KV cache."""
        try:
            extra = self.model_config.get("extra", {})
            model = self.model_config.get("model", self.model)
            messages = self.history_manager.modify_history("hi")
            client.chat.completions.create(
                model=model,
                messages=messages,
                **extra,
            )
            self.logger.info(f"LLM init call OK (messages: {len(messages)})")
        except Exception as e:
            self.logger.error(f"LLM init call failed: {e}")

    def create_stream(self, history):
        extra = self.model_config.get("extra", {})
        model = self.model_config.get("model", self.model)
        result = self.client.chat.completions.create(
            model=model,
            messages=history,
            tools=self.toolsCaller.tools,
            stream=True,
            **extra,
        )
        return result

    def generate_result(self, history):
        result = self.create_stream(history)
        has_tool_call = False
        self.toolsCaller.reset()
        for chunk in result:
            delta = chunk.choices[0].delta
            yield None

            tool_calls = delta.tool_calls
            if tool_calls is not None:
                has_tool_call = True
                tool_call = tool_calls[0]
                self.toolsCaller.update_tool_call(tool_call)

            text = delta.content
            if text is None or has_tool_call:
                continue
            cut_result = self.cutter.cut(text)
            for response in cut_result:
                yield response

        final_result = self.cutter.cut_last()
        for response in final_result:
            yield response

        if has_tool_call:
            tool_calls_list = self.toolsCaller.tool_calls_list()
            results = self.toolsCaller.tool_calls_result()

            if len(tool_calls_list) > 0 and len(results) > 0:
                self.logger.info(
                    f"Tool calls: {tool_calls_list}, Results: {results}"
                )
                yield {
                    "tool_calls": tool_calls_list,
                    "results": results,
                }


class OpenaiStep(LLMStep):
    def custom_init(self):
        self.llm_caller = OpenaiCaller(self.client_id, self.config, self.logger)

    def process(self, data, pass_data={}):
        prompt = data.get("prompt", "")
        sos_signal = {"signal": "SoS"}
        self.add_output(sos_signal, "language", "auto")
        self.output_to_queue(sos_signal, pass_data)

        current_loop = 0
        already_end = False
        while not already_end and current_loop < self.config.get("loop_num", 5):
            already_end = True
            current_loop += 1
            for response in self.llm_caller.call_stream(prompt):
                if self.check_cancel():
                    self.logger.info("cancel inside loop")
                    break
                if response is None:
                    continue
                if "tool_calls" in response:
                    self.logger.info("tool_calls detected")
                    already_end = False
                    prompt = None
                    continue

                current_data = {}
                for key, value in response.items():
                    self.add_output(current_data, key, value)
                self.output_to_queue(current_data, pass_data)
        eos_signal = {"signal": "EoS"}
        self.output_to_queue(eos_signal, pass_data)
        return
