import os
import json

from ..base.BaseProcessingStep import BaseProcessingStep


class MemoryManagerStep(BaseProcessingStep):
    REQUIRED_CATCH_SIGNALS = ["SoS", "EoS"]

    """
    Tracks LLM responses and decides what to store in memory.
    Catches SoS/EoS to track response boundaries.
    Substantive conversations are stored; trivial ones are skipped.
    """

    def custom_init(self):
        # Observer node. Requires config:
        #   catch_signals: ["SoS", "EoS"], pass_signals: ["SoS", "EoS"]
        # (consume for response tracking, framework relays them downstream)
        self.min_content_length = self.get_config("min_content_length", 20)
        self.max_memory_entries = self.get_config("max_memory_entries", 50)
        self.memory_file = f"history/memory_{self.client_id}.json"
        self.current_response_text = ""
        self.memories = self._load_memories()
        self.logger.info(f"loaded {len(self.memories)} memory entries")

    def process(self, data, pass_data={}):
        signal = data.get("signal", "")

        if signal == "SoS":
            self.current_response_text = ""
            return

        if signal == "EoS":
            self._evaluate_and_store(data.get("timestamp"))
            return

        # Accumulate response text
        input_text = data.get("text", "")
        if input_text:
            self.current_response_text += input_text

        # Forward everything as-is
        output_data = {}
        for key, value in data.items():
            if key not in self.reserved_input_vars:
                self.add_output(output_data, key, value)
        self.output_to_queue(output_data, pass_data)

    def _evaluate_and_store(self, timestamp):
        """Evaluate if the current response is worth remembering."""
        response = self.current_response_text.strip()

        if len(response) < self.min_content_length:
            self.logger.info(
                f"memory skip: too short ({len(response)} chars)"
            )
            return

        # Skip trivial patterns
        trivial_starts = ["你好", "嗯", "哈哈", "谢谢", "再见", "晚安", "早"]
        if any(response.startswith(p) for p in trivial_starts) and len(response) < 30:
            self.logger.info("memory skip: trivial response")
            return

        entry = {
            "timestamp": timestamp,  # conversation (turn) timestamp, from the EoS message
            "response_summary": response[:200],
            "response_length": len(response),
        }
        self.memories.append(entry)

        if len(self.memories) > self.max_memory_entries:
            self.memories = self.memories[-self.max_memory_entries :]

        self._save_memories()
        self.logger.info(f"memory stored ({len(self.memories)} total)")

    def _load_memories(self):
        if os.path.exists(self.memory_file):
            try:
                with open(self.memory_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return []
        return []

    def _save_memories(self):
        os.makedirs("history", exist_ok=True)
        with open(self.memory_file, "w", encoding="utf-8") as f:
            json.dump(self.memories, f, ensure_ascii=False, indent=2)
