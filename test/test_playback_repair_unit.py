"""Unit regressions for the LLM playback-repair wire contract."""

import json
import os
import queue
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Modules.llm_base.LLMStep import LLMStep
from Modules.llm_openai.OpenaiStep import OpenaiStep
from Modules.llm_utils.SimpleHistory import SimpleHistory
from Modules.llm_utils.TavernHistory import TavernHistory
from utils.event_handler import EventHandler


class _Logger:
    def __init__(self):
        self.messages = []

    def info(self, message, *args, **kwargs):
        self.messages.append(message)

    def error(self, message, *args, **kwargs):
        self.messages.append(message)


class _Caller:
    def __init__(self):
        self.cancelled = []

    def cancel(self, message):
        self.cancelled.append(message)


class PlaybackRepairContractTest(unittest.TestCase):
    def setUp(self):
        self._old_cwd = os.getcwd()
        self._tmp = tempfile.TemporaryDirectory()
        os.chdir(self._tmp.name)

        self.history = SimpleHistory("playback_unit", {
            "reset_history": True,
            "history_length": 20,
        })
        self.history.begin_turn("hello", "resp_test")
        self.history.record({"item_id": "item_first", "raw_text": "first"})
        self.history.record({"item_id": "item_second", "raw_text": "second"})
        self.history.commit(interrupted=True)

        self.step = self._make_step(self.history)

    def tearDown(self):
        os.chdir(self._old_cwd)
        self._tmp.cleanup()

    def _saved_history(self):
        with open("history/history_playback_unit.json", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _make_step(history):
        step = object.__new__(LLMStep)
        step.harness = history
        step._last_turn = history.turn_identity()
        step.logger = _Logger()
        return step

    @staticmethod
    def _load_history(client_id):
        with open(f"history/history_{client_id}.json", encoding="utf-8") as f:
            return json.load(f)

    def test_empty_item_id_means_playback_ended_before_first_item(self):
        self.step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_test",
            "item_id": "",
        })

        self.assertIsNone(self.step._last_turn)
        self.assertEqual(self._saved_history(), [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "---interrupted---"},
        ])

    def test_missing_item_id_is_not_treated_as_empty(self):
        before = self._saved_history()

        self.step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_test",
        })

        self.assertIsNotNone(self.step._last_turn)
        self.assertEqual(self._saved_history(), before)
        self.assertTrue(any("missing item_id" in message
                            for message in self.step.logger.messages))

        # Ignoring a malformed report must leave the repair window open for
        # the client's subsequent explicit zero-prefix report.
        self.step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_test",
            "item_id": "",
        })
        self.assertIsNone(self.step._last_turn)
        self.assertEqual(self._saved_history()[-1]["content"],
                         "---interrupted---")

    def test_only_the_exact_empty_string_means_zero_prefix(self):
        before = self._saved_history()

        for invalid in (None, False, 0, " "):
            with self.subTest(item_id=invalid):
                self.step.custom_event({
                    "signal": "playback_complete",
                    "response_id": "resp_test",
                    "item_id": invalid,
                })
                self.assertIsNotNone(self.step._last_turn)
                self.assertEqual(self._saved_history(), before)

    def test_nonempty_item_id_keeps_the_existing_exclusive_semantics(self):
        self.step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_test",
            "item_id": "item_second",
        })

        self.assertIsNone(self.step._last_turn)
        self.assertEqual(self._saved_history(), [
            {"role": "user", "content": "hello"},
            {"role": "assistant",
             "content": "first\n---interrupted---"},
        ])

    def test_openai_step_uses_the_same_playback_handler(self):
        self.assertIs(OpenaiStep.custom_event, LLMStep.custom_event)

    def test_first_item_id_also_keeps_zero_items(self):
        self.step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_test",
            "item_id": "item_first",
        })

        self.assertEqual(self._saved_history()[-1]["content"],
                         "---interrupted---")

    def test_empty_item_id_preserves_history_before_the_current_turn(self):
        prior = [
            {"role": "user", "content": "old question"},
            {"role": "assistant", "content": "old answer"},
        ]
        history = SimpleHistory("prior", {
            "reset_history": True,
            "history_length": 20,
        })
        history.save_history(prior)
        history.begin_turn("new question", "resp_prior")
        history.record({"item_id": "item_new", "raw_text": "new answer"})
        history.commit(interrupted=True)
        step = self._make_step(history)

        step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_prior",
            "item_id": "",
        })

        self.assertEqual(self._load_history("prior"), prior + [
            {"role": "user", "content": "new question"},
            {"role": "assistant", "content": "---interrupted---"},
        ])

    def test_tavern_history_has_the_same_zero_prefix_shape(self):
        prior = [
            {"role": "user", "content": "old question"},
            {"role": "assistant", "content": "old answer"},
        ]
        os.makedirs("history", exist_ok=True)
        with open("history/history_tavern.json", "w", encoding="utf-8") as f:
            json.dump(prior, f)
        history = TavernHistory("tavern", {
            "reset_history": False,
            "history_length": 20,
            "lorebooks": [],
        }, _Logger())
        history.begin_turn("new question", "resp_tavern")
        history.record({"item_id": "item_new", "raw_text": "new answer"})
        history.commit(interrupted=True)
        step = self._make_step(history)

        step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_tavern",
            "item_id": "",
        })

        self.assertEqual(self._load_history("tavern"), prior + [
            {"role": "user", "content": "new question"},
            {"role": "assistant", "content": "---interrupted---"},
        ])

    def test_zero_prefix_keeps_tool_bookkeeping_before_first_item(self):
        history = SimpleHistory("tool", {
            "reset_history": True,
            "history_length": 20,
        })
        tool_calls = [{
            "id": "call_test",
            "type": "function",
            "function": {"name": "lookup", "arguments": "{}"},
        }]
        tool_result = {
            "role": "tool",
            "tool_call_id": "call_test",
            "content": "result",
        }
        history.begin_turn("question", "resp_tool")
        history.record({"tool_calls": tool_calls, "results": [tool_result]})
        history.record({"item_id": "item_after_tool",
                        "raw_text": "spoken answer"})
        history.commit(interrupted=True)
        step = self._make_step(history)

        step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_tool",
            "item_id": "",
        })

        self.assertEqual(self._load_history("tool"), [
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "", "tool_calls": tool_calls},
            tool_result,
            {"role": "assistant", "content": "---interrupted---"},
        ])

    def test_wrong_response_with_empty_item_id_is_ignored(self):
        before = self._saved_history()

        self.step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_other",
            "item_id": "",
        })

        self.assertIsNotNone(self.step._last_turn)
        self.assertEqual(self._saved_history(), before)

    def test_second_empty_report_is_stale_after_zero_prefix_repair(self):
        report = {
            "signal": "playback_complete",
            "response_id": "resp_test",
            "item_id": "",
        }
        self.step.custom_event(report)
        repaired = self._saved_history()

        self.step.custom_event(report)

        self.assertIsNone(self.step._last_turn)
        self.assertEqual(self._saved_history(), repaired)

    def test_cancel_then_empty_report_refines_in_one_control_queue_drain(self):
        history = SimpleHistory("queued", {
            "reset_history": True,
            "history_length": 20,
        })
        history.begin_turn("question", "resp_queued")
        history.record({"item_id": "item_generated",
                        "raw_text": "not played"})

        step = object.__new__(LLMStep)
        step.harness = history
        step._last_turn = None
        step.logger = _Logger()
        step.llm_caller = _Caller()
        step.cancel_queue = queue.Queue()
        step.cancel_timestamp = 0
        step.current_timestamp = 10
        step._killed = False
        step.catch_event_map = {"playback_complete": "playback_complete"}
        step.cancel_queue.put(json.dumps({
            "signal": "cancel",
            "timestamp": 11,
        }))
        step.cancel_queue.put(json.dumps({
            "signal": "playback_complete",
            "response_id": "resp_queued",
            "item_id": "",
            "timestamp": 11,
        }))

        self.assertTrue(step.check_cancel())

        self.assertTrue(step.cancel_queue.empty())
        self.assertIsNone(step.current_timestamp)
        self.assertIsNone(step._last_turn)
        self.assertEqual(len(step.llm_caller.cancelled), 1)
        self.assertEqual(self._load_history("queued"), [
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "---interrupted---"},
        ])

    def test_cancel_before_any_server_item_writes_no_turn(self):
        history = SimpleHistory("no_item", {
            "reset_history": True,
            "history_length": 20,
        })
        history.begin_turn("question", "resp_no_item")
        step = object.__new__(LLMStep)
        step.harness = history
        step._last_turn = None
        step.logger = _Logger()
        step.llm_caller = _Caller()
        step.current_timestamp = 10

        step.custom_cancel({"signal": "cancel", "timestamp": 11})
        step.custom_event({
            "signal": "playback_complete",
            "response_id": "resp_no_item",
            "item_id": "",
        })

        self.assertIsNone(step.current_timestamp)
        self.assertIsNone(step._last_turn)
        self.assertFalse(os.path.exists("history/history_no_item.json"))

    def test_event_handler_preserves_empty_id_and_cancel_report_order(self):
        history = SimpleHistory("event_handler", {
            "reset_history": True,
            "history_length": 20,
        })
        history.begin_turn("question", "resp_event_handler")
        history.record({"item_id": "item_generated",
                        "raw_text": "not played"})

        control_queue = queue.Queue()
        handler = EventHandler({1: control_queue}, queue.Queue())
        handler.start()
        handler.submit({"signal": "cancel", "timestamp": 11, "source": 0})
        handler.submit({
            "signal": "playback_complete",
            "response_id": "resp_event_handler",
            "item_id": "",
            "timestamp": 11,
            "source": 0,
        })
        handler.submit({"signal": "kill"})
        handler.join()

        step = object.__new__(LLMStep)
        step.harness = history
        step._last_turn = None
        step.logger = _Logger()
        step.llm_caller = _Caller()
        step.cancel_queue = control_queue
        step.cancel_timestamp = 0
        step.current_timestamp = 10
        step._killed = False
        step.catch_event_map = {"playback_complete": "playback_complete"}

        self.assertTrue(step.check_cancel())

        self.assertTrue(control_queue.empty())
        self.assertTrue(step._killed)
        self.assertIsNone(step._last_turn)
        self.assertEqual(self._load_history("event_handler")[-1]["content"],
                         "---interrupted---")


if __name__ == "__main__":
    unittest.main()
