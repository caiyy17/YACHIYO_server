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
        # Resolve {{variable}} macros in all messages
        modified_history = self._resolve_macros(modified_history)
        return modified_history

    def _resolve_macros(self, history):
        from .Tools import resolve_variables
        static_vars = self.config.get("vars", {})
        result = []
        for msg in history:
            if isinstance(msg, str):
                # TavernHistory flattens lorebook entries to plain strings
                if "{{" in msg:
                    msg = resolve_variables(msg, static_vars)
            elif isinstance(msg, dict) and "content" in msg and "{{" in msg["content"]:
                msg = dict(msg)
                msg["content"] = resolve_variables(msg["content"], static_vars)
            result.append(msg)
        return result

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

    # ── Turn lifecycle (the harness side) ────────────────────────────
    # The step orchestrates: begin_turn -> per round (assemble ->
    # generate -> record what it EMITS) -> commit exactly once at turn
    # end (EoS or cancel) -> refine when a playback report arrives.
    # Nothing touches the file between begin_turn and commit; tool
    # rounds see earlier segments through assemble()'s pending tail.

    def begin_turn(self, prompt, response_id=None):
        """Open a turn: load the persistent history, reset the buffer.
        response_id is the turn's opaque generation identity — kept here
        so turn_identity() can rebuild the repair window from the buffer
        alone (e.g. inside the cancel hook)."""
        self.load_history()
        self.extra_info = {"prompt": prompt}
        self.response_id = response_id
        self._turn_segments = []   # emitted chunks + tool bookkeeping
        self._turn_span = None     # committed range start, set by commit

    def record(self, chunk):
        """Buffer one chunk the step actually consumed: {"item_id",
        "raw_text"} for an emitted sentence, {"tool_calls", "results"}
        for a tool round. (History never stores ids — _segment_messages
        reads only raw_text/tool keys; the ids exist for turn_identity.)"""
        self._turn_segments.append(chunk)

    def turn_recorded(self):
        return bool(self._turn_segments)

    def turn_identity(self):
        """The repair-window view of this turn: its response_id and the
        emitted (item_id, raw_text) pairs, in emission order."""
        return {"response_id": self.response_id,
                "items": [(c["item_id"], c["raw_text"])
                          for c in self._turn_segments
                          if "raw_text" in c and "item_id" in c]}

    def assemble(self):
        """Request messages for the next generation round: the persistent
        history shaped as before (system prompt / lorebooks / macros via
        modify_history) plus this turn's pending segments — the in-memory
        carrier between tool rounds."""
        messages = self.modify_history(self.extra_info.get("prompt"))
        return messages + self._segment_messages(self._turn_segments)

    def commit(self, interrupted=False):
        """The turn's single write: user prompt + the buffered segments
        (interrupted appends the marker to the trailing assistant text).
        Records the turn's span so refine() can rebuild it."""
        history = list(self.current_history)
        start = len(history)
        prompt = self.extra_info.get("prompt")
        if prompt is not None:
            history.append({"role": "user", "content": f"{prompt}"})
        history.extend(self._marked(
            self._segment_messages(self._turn_segments), interrupted))
        self._turn_span = start
        self.save_history(history)
        self.current_history = history

    def refine(self, kept):
        """Playback report: rebuild the committed turn as the played
        prefix — the first `kept` sentences plus the tool bookkeeping
        before the cut; everything after is dropped and the marker
        appended. Returns False when no turn is committed."""
        if self._turn_span is None:
            return False
        segments, n = [], 0
        for chunk in self._turn_segments:
            if "raw_text" in chunk:
                if n >= kept:
                    break
                n += 1
            segments.append(chunk)
        history = list(self.current_history)[:self._turn_span]
        prompt = self.extra_info.get("prompt")
        if prompt is not None:
            history.append({"role": "user", "content": f"{prompt}"})
        history.extend(self._marked(
            self._segment_messages(segments), True))
        self.save_history(history)
        self.current_history = history
        return True

    @staticmethod
    def _segment_messages(chunks):
        """Buffered chunks -> chat messages (the one place owning the
        turn's message shape): consecutive raw_text merges into one
        assistant message; a tool chunk closes it with its tool_calls
        field and appends the tool results."""
        messages, current = [], None
        for item in chunks:
            if "raw_text" in item:
                if current is None:
                    current = {"role": "assistant", "content": ""}
                current["content"] += item["raw_text"]
            elif "tool_calls" in item:
                if current is None:
                    current = {"role": "assistant", "content": ""}
                current["tool_calls"] = item["tool_calls"]
                messages.append(current)
                current = None
                messages.extend(item["results"])
        if current is not None:
            messages.append(current)
        return messages

    @staticmethod
    def _marked(messages, interrupted):
        """Append the interruption marker to the trailing assistant text
        (a marker-only message when the cut left no text)."""
        if not interrupted:
            return messages
        if messages and messages[-1].get("role") == "assistant" \
                and "tool_calls" not in messages[-1]:
            messages = messages[:-1] + [{**messages[-1], "content":
                messages[-1]["content"] + "\n---interrupted---"}]
        else:
            messages = messages + [{"role": "assistant",
                                    "content": "---interrupted---"}]
        return messages

    def rewrite_last_assistant(self, content):
        """Rewrite the newest assistant message of the saved history (the
        playback-repair hook). Returns False when none exists."""
        self.load_history()
        history = list(self.current_history)
        idx = next((i for i in range(len(history) - 1, -1, -1)
                    if isinstance(history[i], dict)
                    and history[i].get("role") == "assistant"), None)
        if idx is None:
            return False
        history[idx] = {**history[idx], "content": content}
        self.save_history(history)
        self.current_history = history
        return True

    def cancel(self, cancel_message):
        pass
