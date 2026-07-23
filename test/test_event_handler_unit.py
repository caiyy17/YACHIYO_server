import json
import queue
import sys
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.event_handler import CANCEL_EPSILON, EventHandler


class EventHandlerRoutingTest(unittest.TestCase):
    def test_source_is_consumed_before_node_and_client_dispatch(self):
        node_one = queue.Queue()
        node_two = queue.Queue()
        client = queue.Queue()
        handler = EventHandler({1: node_one, 2: node_two}, client)
        handler.start()

        handler.submit({
            "signal": "cancel",
            "timestamp": 10.0,
            "source": 1,
            "response_id": "response-1",
        })
        handler.submit({
            "signal": "playback_complete",
            "timestamp": 11.0,
            "source": 2,
            "item_id": "item-1",
        })
        handler.submit({
            "signal": "kill",
            "timestamp": 12.0,
            "source": 1,
        })
        handler.join()

        self.assertFalse(handler._thread.is_alive())

        node_one_messages = [
            json.loads(node_one.get_nowait()),
            json.loads(node_one.get_nowait()),
        ]
        node_two_messages = [
            json.loads(node_two.get_nowait()),
            json.loads(node_two.get_nowait()),
        ]
        client_message = json.loads(client.get_nowait())

        self.assertEqual(
            [message["signal"] for message in node_one_messages],
            ["playback_complete", "kill"],
        )
        self.assertEqual(
            [message["signal"] for message in node_two_messages],
            ["cancel", "kill"],
        )
        self.assertAlmostEqual(
            node_two_messages[0]["timestamp"],
            10.0 - CANCEL_EPSILON,
        )
        self.assertEqual(node_two_messages[0]["response_id"], "response-1")
        self.assertEqual(client_message, node_two_messages[0])

        for message in (
            node_one_messages + node_two_messages + [client_message]
        ):
            self.assertNotIn("source", message)

        self.assertTrue(node_one.empty())
        self.assertTrue(node_two.empty())
        self.assertTrue(client.empty())

    def test_boundary_cancel_is_not_echoed_and_has_no_source(self):
        node = queue.Queue()
        client = queue.Queue()
        handler = EventHandler({1: node}, client)
        handler.start()

        handler.submit({
            "signal": "cancel",
            "timestamp": 20.0,
            "source": 0,
        })
        handler.submit({"signal": "kill", "timestamp": 21.0})
        handler.join()

        cancel = json.loads(node.get_nowait())
        kill = json.loads(node.get_nowait())
        self.assertEqual(cancel["signal"], "cancel")
        self.assertNotIn("source", cancel)
        self.assertEqual(kill["signal"], "kill")
        self.assertNotIn("source", kill)
        self.assertTrue(client.empty())


if __name__ == "__main__":
    unittest.main()
