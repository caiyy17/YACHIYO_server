import os
import json


class SimpleHistory:
    def __init__(self, id, config):
        self.client_id = id
        self.config = config

        self.reset_history = self.config.get("reset_history", True)
        self.history_length = self.config.get("history_length", 20)  # 10 conversation turns
        self.system_prompt = self.config.get("system_prompt", "")
        if self.reset_history:
            self.clear_history()
        self.current_history = []
        self.extra_info = {}

    def load_history(self):
        if os.path.exists(f"history/history_{self.client_id}.json"):
            with open(
                f"history/history_{self.client_id}.json", "r", encoding="utf-8"
            ) as file:
                history = json.load(file)
        else:
            history = []

        if len(history) > self.history_length:
            history = history[-self.history_length :]
        self.current_history = history

    def save_history(self, history):
        if not os.path.exists("history"):
            os.makedirs("history")
        with open(
            f"history/history_{self.client_id}.json", "w", encoding="utf-8"
        ) as file:
            json.dump(history, file, ensure_ascii=False)

    def clear_history(self):
        if os.path.exists(f"history/history_{self.client_id}.json"):
            os.remove(f"history/history_{self.client_id}.json")
        self.current_history = []

    def modify_history(self, prompt):
        self.extra_info = {}
        self.extra_info["prompt"] = prompt
        modified_history = self.current_history.copy()
        modified_history.insert(
            0, {"role": "system", "content": f"{self.system_prompt}"}
        )
        if prompt is not None:
            modified_history.append({"role": "user", "content": f"{prompt}"})
        return modified_history

    def prepare_saving(self):
        history = self.current_history.copy()
        prompt = self.extra_info["prompt"]
        if prompt is not None:
            history.append({"role": "user", "content": f"{prompt}"})

        current_response = self.extra_info["current_response"]
        current_message = None
        for item in current_response:
            if "raw_text" in item:
                if current_message is None:
                    current_message = {"role": "assistant", "content": ""}
                current_message["content"] += item["raw_text"]
            elif "tool_calls" in item:
                if current_message is None:
                    current_message = {"role": "assistant", "content": ""}
                current_message["tool_calls"] = item["tool_calls"]
                history.append(current_message)
                current_message = None
                for result in item["results"]:
                    history.append(result)
            else:
                pass

        if current_message is not None:
            history.append(current_message)

        return history

    def cancel(self, cancel_message):
        pass
