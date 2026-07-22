import json

from ..llm_base.LLMStep import SimpleHistory, BaseLLMCaller, LLMStep
from ..llm_utils.TavernHistory import TavernHistory
from ..llm_utils.StreamCutter import StreamCutter
from ..llm_utils.ToolsCaller import ToolsCaller


class OpenaiCaller(BaseLLMCaller):
    def custom_init(self):
        self.client = self.create_client()
        self.cutter = StreamCutter(self.config)
        self.toolsCaller = ToolsCaller(self.config, self.logger)

    def cancel(self, cancel_message):
        self.cutter.reset()

    def create_client(self):
        config_name = self.config.get("model", "gpt")
        with open("configs/settings/llm.json", "r") as f:
            self.model_config = json.load(f)[config_name]
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
        self._init_call(client)
        return client

    def _init_call(self, client):
        """Init call with full system prompt to warm up KV cache. The
        warmup messages are assembled by the step's harness manager and
        injected at construction."""
        try:
            model_name = self.model_config.get("model_name", "gpt")
            extra = self.model_config.get("extra", {})
            messages = self.warmup_messages \
                or [{"role": "user", "content": "hi"}]
            client.chat.completions.create(
                model=model_name,
                messages=messages,
                **extra,
            )
            self.logger.info(f"LLM init call OK (messages: {len(messages)})")
        except Exception as e:
            self.logger.error(f"LLM init call failed: {e}")
            raise  # init failure must surface (fail-fast at pipeline init)

    def create_stream(self, history, allow_tools=True):
        model_name = self.model_config.get("model_name", "gpt")
        extra = self.model_config.get("extra", {})
        tool_kwargs = {}
        if self.toolsCaller.tools:
            tool_kwargs["tools"] = self.toolsCaller.tools
            tool_kwargs["tool_choice"] = "auto" if allow_tools else "none"
        result = self.client.chat.completions.create(
            model=model_name,
            messages=history,
            stream=True,
            **tool_kwargs,
            **extra,
        )
        return result

    def generate_result(self, history, allow_tools=True):
        result = self.create_stream(history, allow_tools=allow_tools)
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
        # harness manager per config (assembly + turn history lifecycle);
        # the caller is a pure API adapter and only receives the warmup
        history_mode = self.config.get("history_mode", "simple")
        if history_mode == "tavern":
            self.harness = TavernHistory(
                self.client_id, self.config, self.logger)
        else:
            self.harness = SimpleHistory(self.client_id, self.config)
        self.llm_caller = OpenaiCaller(
            self.client_id, self.config, self.logger,
            warmup_messages=self.harness.modify_history("hi"))
        self._last_turn = None

    def process(self, data, pass_data={}):
        prompt = data.get("prompt", "")
        # Generation identity: response_id spans the whole turn (rides on
        # SoS, every sentence and EoS), item_id is minted per sentence.
        response_id = self.mint_id("resp")
        # the previous turn's repair window closes here
        self._last_turn = None
        self.harness.begin_turn(prompt, response_id)
        # Stream envelope: pass_vars data travels once on the SoS, wrapped
        # under the fixed "pass_data" key (shape built here; emit_signal
        # ships flat); stream messages and EoS carry only the timestamp.
        sos = self.envelope(self.stamp({}, pass_data), pass_data, wrap=True)
        sos["response_id"] = response_id
        self.emit_signal("SoS", sos)
        current_loop = 0
        already_end = False
        loop_num = self.config.get("loop_num", 5)
        while not already_end and current_loop < loop_num:
            already_end = True
            current_loop += 1
            is_last_loop = current_loop >= loop_num
            # assemble per round: history + prompt + this turn's pending
            # segments (tool context travels in memory, not via the file)
            for response in self.llm_caller.generate(
                    self.harness.assemble(),
                    allow_tools=not is_last_loop):
                if self.check_cancel():
                    # the cancel hook already concluded the turn; fast
                    # exit — no EoS, the envelope only closes naturally
                    self.logger.info("cancel inside loop")
                    return
                if response is None:
                    continue
                if "tool_calls" in response:
                    self.logger.info("tool_calls detected")
                    self.harness.record(response)
                    already_end = False
                    continue

                item_id = self.mint_id("item")
                self.harness.record(
                    {"item_id": item_id,
                     "raw_text": response.get("raw_text", "")})
                current_data = {}
                self.add_output(current_data, "response_id", response_id)
                self.add_output(current_data, "item_id", item_id)
                for key, value in response.items():
                    self.add_output(current_data, key, value)
                self.output_to_queue(current_data, pass_data,
                                     is_add_pass_data=False)
        # natural completion: close the envelope, open the repair window,
        # write the turn's single history entry
        eos = self.stamp({}, pass_data)
        eos["response_id"] = response_id
        self.emit_signal("EoS", eos)
        self._last_turn = self.harness.turn_identity()
        self.harness.commit()
        return
