import json
import os
import queue
import sys
import time
import unittest
from unittest.mock import patch


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Modules.danmaku_buffer.DanmakuBufferStep import DanmakuBufferStep


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


TIME_TARGET = "Modules.danmaku_buffer.DanmakuBufferStep.time.time"


def _message(timestamp, text, buffered_at=None):
    return {
        "text": text,
        "user": f"user-{timestamp}",
        "msg_type": "danmaku",
        "priority": 3,
        "timestamp": timestamp,
        "_buffered_at": timestamp if buffered_at is None else buffered_at,
        "guard_level": 0,
        "num": 0,
        "price": 0,
    }


class DanmakuBufferCancelTest(unittest.TestCase):
    @staticmethod
    def _step(**overrides):
        # The cancel/release state machine is self-contained. Avoid starting
        # the BaseProcessingStep thread or constructing unrelated queues.
        step = object.__new__(DanmakuBufferStep)
        step.config = {
            "next_nodes": [-1],
            "release_interval": 0,
            "min_batch_size": 1,
            "max_batch_size": 8,
            "max_wait_time": 60,
        }
        step.config.update(overrides)
        step.logger = _Logger()
        step.current_timestamp = None
        step.cancel_timestamp = 0
        step.cancel_queue = queue.Queue()
        step.catch_event_map = {}
        step._killed = False
        step.output_queue = queue.Queue()
        step.send_queue = queue.Queue()
        step.output_dict = {"prompt": ["prompt"]}
        step.span_init()
        return step

    def test_cancel_filters_mixed_buffer_with_strict_timestamp_boundary(self):
        step = self._step()
        step.buffer = [
            _message(9, "old message"),
            _message(10, "equal boundary message"),
            _message(11, "new message"),
        ]
        step._ctx_ring.extend([
            {"timestamp": 9},
            {"timestamp": 10},
            {"timestamp": 11},
        ])
        step.current_timestamp = 9
        # BaseProcessingStep installs the watermark before invoking the hook.
        step.cancel_timestamp = 10

        step.custom_cancel({"signal": "cancel", "timestamp": 10})

        self.assertEqual(
            [item["timestamp"] for item in step.buffer],
            [10, 11],
        )
        self.assertEqual(
            [ctx["timestamp"] for ctx in step._ctx_ring],
            [10, 11],
        )
        self.assertTrue(step.span_active)
        self.assertEqual(step.current_timestamp, 10)

    def test_cancelled_old_release_keeps_and_releases_new_buffer(self):
        step = self._step()
        step.waiting_for_playback = True
        step.last_release_pts = 9
        step.last_release_time = time.time()
        step.cancel_timestamp = 10
        step.current_timestamp = 10
        step.span_start_time = time.time()
        step.buffer = [
            _message(10, "equal boundary message"),
            _message(11, "new message"),
        ]
        step._ctx_ring.extend([
            {"timestamp": 9},
            {"timestamp": 10},
            {"timestamp": 11},
        ])

        step.custom_update()

        released = json.loads(step.output_queue.get_nowait())
        self.assertEqual(released["timestamp"], 11)
        self.assertIn("equal boundary message", released["prompt"])
        self.assertIn("new message", released["prompt"])
        self.assertEqual(step.buffer, [])
        self.assertEqual(step.total_released, 2)
        self.assertFalse(step.span_active)
        # The cancelled old release was unlocked; the newly released batch now
        # owns the playback wait.
        self.assertTrue(step.waiting_for_playback)
        self.assertEqual(step.last_release_pts, 11)
        self.assertTrue(step.output_queue.empty())

    def test_cancel_rebases_max_wait_to_first_survivor_arrival(self):
        step = self._step(
            min_batch_size=99,
            release_interval=999,
            max_wait_time=30,
        )
        with patch(TIME_TARGET, return_value=100):
            step.span_process(_message(9, "old message"))
        with patch(TIME_TARGET, return_value=120):
            step.span_process(_message(11, "surviving message"))

        step.cancel_queue.put(json.dumps({
            "signal": "cancel",
            "timestamp": 10,
        }))
        with patch(TIME_TARGET, return_value=121):
            step.check_cancel()

        self.assertEqual(
            [item["timestamp"] for item in step.buffer],
            [11],
        )
        self.assertEqual(step.current_timestamp, 11)
        self.assertEqual(step.span_start_time, 120)

        with patch(TIME_TARGET, return_value=149):
            step.custom_update()
        self.assertTrue(step.output_queue.empty())

        with patch(TIME_TARGET, return_value=150):
            step.custom_update()
        released = json.loads(step.output_queue.get_nowait())
        self.assertEqual(released["timestamp"], 11)
        self.assertIn("surviving message", released["prompt"])

    def test_empty_cancel_then_new_message_starts_fresh_max_wait(self):
        step = self._step(
            min_batch_size=99,
            release_interval=999,
            max_wait_time=30,
        )
        with patch(TIME_TARGET, return_value=100):
            step.span_process(_message(9, "old message"))

        step.cancel_queue.put(json.dumps({
            "signal": "cancel",
            "timestamp": 10,
        }))
        with patch(TIME_TARGET, return_value=110):
            step.check_cancel()
        self.assertEqual(step.buffer, [])
        self.assertEqual(step.span_start_time, 0)

        with patch(TIME_TARGET, return_value=125):
            step.span_process(_message(11, "fresh message"))
        self.assertEqual(step.span_start_time, 125)

        with patch(TIME_TARGET, return_value=154):
            step.custom_update()
        self.assertTrue(step.output_queue.empty())
        with patch(TIME_TARGET, return_value=155):
            step.custom_update()
        self.assertIn(
            "fresh message",
            json.loads(step.output_queue.get_nowait())["prompt"],
        )

    def test_partial_release_rebases_max_wait_to_first_remainder(self):
        step = self._step(
            min_batch_size=99,
            max_batch_size=2,
            release_interval=999,
            max_wait_time=30,
        )
        for wall_time, timestamp, text in (
            (100, 10, "first message"),
            (110, 11, "second message"),
            (120, 12, "remainder message"),
        ):
            with patch(TIME_TARGET, return_value=wall_time):
                step.span_process(_message(timestamp, text))

        with patch(TIME_TARGET, return_value=121):
            step._release_batch()
        step.output_queue.get_nowait()
        self.assertEqual(
            [item["timestamp"] for item in step.buffer],
            [12],
        )
        self.assertEqual(step.span_start_time, 120)

        # Simulate the matching playback_complete without changing the
        # remainder's own max-wait clock.
        step.waiting_for_playback = False
        with patch(TIME_TARGET, return_value=149):
            step.custom_update()
        self.assertTrue(step.output_queue.empty())
        with patch(TIME_TARGET, return_value=150):
            step.custom_update()
        self.assertIn(
            "remainder message",
            json.loads(step.output_queue.get_nowait())["prompt"],
        )

    def test_inactive_cancel_restarts_idle_timer_through_real_queue_path(self):
        step = self._step(idle_talk_interval=30)
        step.waiting_for_playback = True
        step.last_release_pts = 9
        step.last_release_time = 100
        step.idle_start_time = 100
        self.assertFalse(step.span_active)
        step.cancel_queue.put(json.dumps({
            "signal": "cancel",
            "timestamp": 10,
        }))

        with patch(TIME_TARGET, return_value=120):
            step.check_cancel()
        self.assertFalse(step.waiting_for_playback)
        self.assertEqual(step.idle_start_time, 120)

        with patch(TIME_TARGET, return_value=149):
            step.custom_update()
        self.assertTrue(step.output_queue.empty())
        with patch(TIME_TARGET, return_value=150):
            step.custom_update()
        idle = json.loads(step.output_queue.get_nowait())
        self.assertEqual(idle["timestamp"], 10)
        self.assertEqual(idle["prompt"], "（当前没有新弹幕）")

    def test_duplicate_or_older_cancel_does_not_restart_idle_timer(self):
        step = self._step(idle_talk_interval=30)
        step.idle_start_time = 100
        step.cancel_queue.put(json.dumps({
            "signal": "cancel",
            "timestamp": 10,
        }))
        with patch(TIME_TARGET, return_value=120):
            step.check_cancel()
        self.assertEqual(step.idle_start_time, 120)

        for timestamp in (10, 9):
            step.cancel_queue.put(json.dumps({
                "signal": "cancel",
                "timestamp": timestamp,
            }))
        with patch(TIME_TARGET, return_value=140):
            step.check_cancel()
        self.assertEqual(step.idle_start_time, 120)

    def test_post_cancel_message_and_event_become_the_new_activity_clock(self):
        step = self._step(
            min_batch_size=99,
            release_interval=999,
            max_wait_time=999,
        )
        step.cancel_queue.put(json.dumps({
            "signal": "cancel",
            "timestamp": 10,
        }))
        with patch(TIME_TARGET, return_value=120):
            step.check_cancel()
        self.assertEqual(step.idle_start_time, 120)

        with patch(TIME_TARGET, return_value=125):
            step.span_process(_message(11, "post cancel message"))
        self.assertEqual(step.idle_start_time, 125)
        self.assertEqual(step.span_start_time, 125)

        with patch(TIME_TARGET, return_value=130):
            step.custom_event({
                "signal": "playback_complete",
                "timestamp": 12,
                "last_batch_timestamp": 0,
            })
        self.assertEqual(step.idle_start_time, 130)
        self.assertEqual(step.span_start_time, 125)
        self.assertTrue(step.output_queue.empty())

    def test_cancel_preserves_actual_release_interval_then_new_release_restarts_it(self):
        step = self._step(
            min_batch_size=2,
            release_interval=30,
            max_wait_time=999,
        )
        step.waiting_for_playback = True
        step.last_release_pts = 9
        step.last_release_time = 100
        step.buffer = [
            _message(10, "equal boundary message", 119),
            _message(11, "new message", 120),
        ]
        step._ctx_ring.extend([
            {"timestamp": 9},
            {"timestamp": 10},
            {"timestamp": 11},
        ])
        step._sync_span_to_buffer()
        step.cancel_queue.put(json.dumps({
            "signal": "cancel",
            "timestamp": 10,
        }))

        with patch(TIME_TARGET, return_value=121):
            step.check_cancel()
            step.custom_update()
        self.assertFalse(step.waiting_for_playback)
        self.assertTrue(step.output_queue.empty())

        with patch(TIME_TARGET, return_value=130):
            step.custom_update()
        first = json.loads(step.output_queue.get_nowait())
        self.assertIn("new message", first["prompt"])
        self.assertEqual(step.last_release_time, 130)

        step.waiting_for_playback = False
        with patch(TIME_TARGET, return_value=131):
            step.span_process(_message(12, "next batch one"))
        with patch(TIME_TARGET, return_value=132):
            step.span_process(_message(13, "next batch two"))
        with patch(TIME_TARGET, return_value=159):
            step.custom_update()
        self.assertTrue(step.output_queue.empty())
        with patch(TIME_TARGET, return_value=160):
            step.custom_update()
        second = json.loads(step.output_queue.get_nowait())
        self.assertIn("next batch one", second["prompt"])
        self.assertIn("next batch two", second["prompt"])


if __name__ == "__main__":
    unittest.main()
