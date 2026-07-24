import base64
import json
import logging
import os
import queue
import socket
import sys
import threading
import time
import unittest

import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Modules.base.ChunkGenerationStep import (  # noqa: E402
    ChunkGenerationCancelled,
)
from Modules.motion_generation.MotionChunkGenerationStep import (  # noqa: E402
    MotionWebSocketError,
    MotionWebSocketSession,
    MotionChunkGenerationStep,
)
from Modules.motion_generation.MotionGenerationStep import (  # noqa: E402
    _humanoid_to_frames,
)
from Modules.motion_generation.smplh_to_humanoid import (  # noqa: E402
    smplh_to_humanoid,
)


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def _b64(array):
    return base64.b64encode(
        np.ascontiguousarray(array, dtype=np.float32).tobytes()
    ).decode("ascii")


def _delta(start, count):
    ids = np.arange(start, start + count, dtype=np.float32)
    poses = np.zeros((count, 156), dtype=np.float32)
    poses[:, 0] = ids
    trans = np.zeros((count, 3), dtype=np.float32)
    trans[:, 0] = ids
    return {
        "type": "motion.delta",
        "num_frames": count,
        "poses": _b64(poses),
        "poses_shape": [count, 156],
        "trans": _b64(trans),
        "trans_shape": [count, 3],
    }


class _Protocol:
    """No-model implementation of Flood's exact JSON event protocol."""

    def __init__(self, first_count=7, continue_count=6):
        self.first_count = first_count
        self.continue_count = continue_count
        self.next_frame = 0
        self.messages = []

    def handle(self, raw):
        message = json.loads(raw)
        self.messages.append(message)
        if message.get("type") == "start":
            self.next_frame = 0
            count = self.first_count
            events = [{
                "type": "session.started",
                "format": "smplh",
                "framerate": 30,
                "stream_size": message.get("stream_size"),
                "feature_dim": 159,
            }, _delta(self.next_frame, count)]
        elif message.get("type") == "continue":
            count = self.continue_count
            events = [_delta(self.next_frame, count)]
        else:
            return [{
                "type": "error",
                "error": {"message": "expected start|continue"},
            }]
        self.next_frame += count
        return events


class _FakeConnection:
    def __init__(self, protocol):
        self.protocol = protocol
        self.responses = queue.Queue()
        self.closed = False

    def send(self, raw):
        if self.closed:
            raise RuntimeError("closed")
        for event in self.protocol.handle(raw):
            self.responses.put(json.dumps(event))

    def recv(self, timeout=None):
        if self.closed:
            raise RuntimeError("closed")
        try:
            return self.responses.get(timeout=timeout)
        except queue.Empty as error:
            raise TimeoutError from error

    def close(self):
        self.closed = True


class _FakeConnector:
    def __init__(self, protocol):
        self.protocol = protocol
        self.calls = 0
        self.connections = []

    def __call__(self, uri, **kwargs):
        self.calls += 1
        connection = _FakeConnection(self.protocol)
        self.connections.append(connection)
        return connection


def _session(connector, **kwargs):
    humanoid_output = kwargs.pop("humanoid_output", False)
    return MotionWebSocketSession(
        "ws://fake.invalid:18084/ws",
        6,
        30,
        logger=_Logger(),
        humanoid_output=humanoid_output,
        connector=connector,
        receive_timeout=1,
        poll_interval=0.01,
        **kwargs,
    )


def _frame_ids(chunk):
    return [int(frame["trans"][0]) for frame in chunk["motion"]]


class MotionWebSocketMemoryTest(unittest.TestCase):
    def test_one_request_per_input_uses_sos_motion_hint(self):
        protocol = _Protocol()
        connector = _FakeConnector(protocol)
        session = _session(connector)
        try:
            session.reset({"motion_hint": "walk"})
            chunks = [
                session.generate_chunk({
                    "prompt": f"ignored prompt {index}",
                    "motion_hint": f"ignored hint {index}",
                }, index)
                for index in range(4)
            ]
            self.assertEqual([len(chunk["motion"]) for chunk in chunks],
                             [6, 6, 6, 6])
            self.assertEqual(
                [_frame_ids(chunk) for chunk in chunks],
                [list(range(i, i + 6)) for i in range(1, 25, 6)],
            )
            self.assertEqual(connector.calls, 1)
            self.assertEqual(
                [message["type"] for message in protocol.messages],
                ["start", "continue", "continue", "continue"],
            )
            self.assertEqual(
                [message["text"] for message in protocol.messages],
                ["walk"] * 4,
            )
            self.assertEqual(protocol.messages[0]["stream_size"], 6)
            self.assertEqual(
                chunks[0]["motion"][0]["header"],
                {"framerate": 30, "format": "smplh"},
            )
            self.assertNotIn("header", chunks[1]["motion"][0])
        finally:
            session.close()

    def test_empty_and_whitespace_hints_are_sent_verbatim(self):
        protocol = _Protocol()
        connector = _FakeConnector(protocol)
        session = _session(connector)
        try:
            session.reset({"motion_hint": ""})
            session.generate_chunk({}, 0)
            session.finish()
            session.reset({"motion_hint": "   "})
            session.generate_chunk({}, 0)
            self.assertEqual(
                [message["text"] for message in protocol.messages],
                ["", "   "],
            )
        finally:
            session.close()

    def test_finish_releases_connection_and_next_reset_reconnects(self):
        protocol = _Protocol()
        connector = _FakeConnector(protocol)
        session = _session(connector)
        try:
            session.reset({"motion_hint": "walk"})
            first = session.generate_chunk({}, 0)
            first_connection = connector.connections[0]
            session.finish()
            self.assertTrue(first_connection.closed)

            session.reset({"motion_hint": "wave"})
            reset = session.generate_chunk({}, 0)
            self.assertEqual(connector.calls, 2)
            self.assertFalse(connector.connections[1].closed)
            self.assertEqual(_frame_ids(first), list(range(1, 7)))
            self.assertEqual(_frame_ids(reset), list(range(1, 7)))
            self.assertIn("header", reset["motion"][0])
            self.assertEqual(
                [message["type"] for message in protocol.messages],
                ["start", "start"],
            )
            self.assertEqual(
                [message["text"] for message in protocol.messages],
                ["walk", "wave"],
            )
        finally:
            session.close()

    def test_incremental_humanoid_conversion_matches_whole_stream(self):
        protocol = _Protocol()
        session = _session(
            _FakeConnector(protocol), humanoid_output=True
        )
        try:
            session.reset({"motion_hint": "walk"})
            actual = []
            for index in range(4):
                actual.extend(session.generate_chunk({}, index)["motion"])
            actual[0] = {
                key: value for key, value in actual[0].items()
                if key != "header"
            }

            # Convert the complete raw timeline, including Flood's frame-0
            # bootstrap anchor, then compare only the 24 emitted frames.
            poses = np.zeros((25, 156), dtype=np.float32)
            poses[:, 0] = np.arange(25, dtype=np.float32)
            trans = np.zeros((25, 3), dtype=np.float32)
            trans[:, 0] = np.arange(25, dtype=np.float32)
            expected = _humanoid_to_frames(smplh_to_humanoid(
                poses, trans, 25, framerate=30
            ))[1:]
            for got, want in zip(actual, expected):
                np.testing.assert_allclose(got["root_dxz"], want["root_dxz"])
                np.testing.assert_allclose(got["hips_pos"], want["hips_pos"])
                self.assertEqual(got["joints"].keys(), want["joints"].keys())
                for joint in got["joints"]:
                    np.testing.assert_allclose(
                        got["joints"][joint], want["joints"][joint]
                    )
        finally:
            session.close()

    def test_pipelined_backlog_pairs_replies_in_order(self):
        protocol = _Protocol()
        connector = _FakeConnector(protocol)
        session = _session(connector)
        try:
            session.reset({"motion_hint": "walk"})
            for index in range(3):
                session.submit({}, index)
            self.assertTrue(session.has_pending())
            chunks = session.poll()
            while session.has_pending():
                chunks.append(session.next_result())
            self.assertEqual(
                [_frame_ids(chunk) for chunk in chunks],
                [list(range(i, i + 6)) for i in range(1, 18, 6)],
            )
            self.assertEqual(
                [message["type"] for message in protocol.messages],
                ["start", "continue", "continue"],
            )
            self.assertIn("header", chunks[0]["motion"][0])
            self.assertNotIn("header", chunks[1]["motion"][0])
            self.assertFalse(session.has_pending())
        finally:
            session.close()

    def test_abort_discards_in_flight_and_next_reset_reconnects(self):
        protocol = _Protocol()
        connector = _FakeConnector(protocol)
        session = _session(connector)
        try:
            session.reset({"motion_hint": "walk"})
            session.submit({}, 0)
            session.submit({}, 1)
            self.assertTrue(session.has_pending())
            session.abort()
            self.assertFalse(session.has_pending())
            self.assertTrue(connector.connections[0].closed)

            session.reset({"motion_hint": "wave"})
            recovered = session.generate_chunk({}, 0)
            self.assertEqual(connector.calls, 2)
            self.assertEqual(_frame_ids(recovered), list(range(1, 7)))
        finally:
            session.close()

    def test_abort_closes_and_next_reset_reconnects(self):
        connector = _FakeConnector(_Protocol())
        session = _session(connector)
        first_connection = connector.connections[0]
        session.abort()
        session.abort()
        self.assertTrue(first_connection.closed)
        session.reset({"motion_hint": "walk"})
        self.assertEqual(connector.calls, 2)
        session.close()

    def test_cancel_poll_closes_blocked_receive(self):
        protocol = _Protocol()
        connector = _FakeConnector(protocol)
        checks = iter((False, False, True))
        session = _session(
            connector,
            cancel_check=lambda: next(checks, True),
        )
        # Keep session.started but remove the following delta, forcing the
        # receive loop to reach its next cooperative cancellation check.
        original = protocol.handle

        def only_started(raw):
            events = original(raw)
            return events[:1]

        protocol.handle = only_started
        try:
            session.reset({"motion_hint": "walk"})
            with self.assertRaises(ChunkGenerationCancelled):
                session.generate_chunk({}, 0)
            self.assertTrue(connector.connections[0].closed)
        finally:
            session.close()

    def test_protocol_error_and_bad_shape_are_rejected(self):
        protocol = _Protocol()
        connector = _FakeConnector(protocol)
        session = _session(connector)
        try:
            original = protocol.handle
            protocol.handle = lambda raw: [{
                "type": "error",
                "error": {"message": "bad request"},
            }]
            session.reset({"motion_hint": "walk"})
            with self.assertRaisesRegex(MotionWebSocketError, "bad request"):
                session.generate_chunk({}, 0)
            self.assertTrue(connector.connections[0].closed)

            protocol.handle = original
            session.reset({"motion_hint": "walk"})
            recovered = session.generate_chunk({}, 0)
            self.assertEqual(_frame_ids(recovered), list(range(1, 7)))
            self.assertEqual(connector.calls, 2)
        finally:
            session.close()

        protocol = _Protocol()
        connector = _FakeConnector(protocol)
        session = _session(connector)
        original = protocol.handle

        def bad_shape(raw):
            events = original(raw)
            events[-1]["poses_shape"] = [events[-1]["num_frames"], 155]
            return events

        protocol.handle = bad_shape
        try:
            session.reset({"motion_hint": "walk"})
            with self.assertRaisesRegex(MotionWebSocketError, "shape"):
                session.generate_chunk({}, 0)
        finally:
            session.close()

    def test_non_exact_server_block_is_rejected(self):
        connector = _FakeConnector(_Protocol(first_count=9, continue_count=8))
        session = _session(connector)
        try:
            session.reset({"motion_hint": "walk"})
            with self.assertRaisesRegex(
                    MotionWebSocketError, "returned 9 frames"):
                session.generate_chunk({}, 0)
            self.assertTrue(connector.connections[0].closed)
        finally:
            session.close()


class _LoopbackFloodServer:
    def __init__(self):
        from websockets.sync.server import serve

        self.protocols = []
        self._lock = threading.Lock()
        self.opened = 0
        self.closed = 0
        self.active = 0
        self.listener = socket.create_server(("127.0.0.1", 0))
        host, port = self.listener.getsockname()
        self.url = f"ws://{host}:{port}"
        self.server = serve(self._handler, sock=self.listener)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

    def _handler(self, connection):
        protocol = _Protocol()
        with self._lock:
            self.protocols.append(protocol)
            self.opened += 1
            self.active += 1
        try:
            for raw in connection:
                for event in protocol.handle(raw):
                    connection.send(json.dumps(event))
        except Exception:
            pass
        finally:
            with self._lock:
                self.closed += 1
                self.active -= 1

    def snapshot(self):
        with self._lock:
            return self.opened, self.closed, self.active

    def close(self):
        self.server.shutdown()
        self.thread.join(timeout=2)
        if self.thread.is_alive():
            raise RuntimeError("loopback websocket server did not stop")


class MotionWebSocketLoopbackTest(unittest.TestCase):
    def test_production_websocket_connector_against_fake_server(self):
        server = _LoopbackFloodServer()
        session = None
        try:
            session = MotionWebSocketSession(
                server.url,
                6,
                30,
                logger=_Logger(),
                humanoid_output=False,
                receive_timeout=2,
                poll_interval=0.02,
            )
            session.reset({"motion_hint": "walk"})
            first = session.generate_chunk({}, 0)
            second = session.generate_chunk({"motion_hint": "ignored"}, 1)
            self.assertEqual(_frame_ids(first), list(range(1, 7)))
            self.assertEqual(_frame_ids(second), list(range(7, 13)))
            self.assertEqual(len(server.protocols), 1)
            self.assertEqual(
                [message["type"] for message in server.protocols[0].messages],
                ["start", "continue"],
            )
            self.assertEqual(
                [message["text"] for message in server.protocols[0].messages],
                ["walk", "walk"],
            )
        finally:
            if session is not None:
                session.close()
            server.close()

    def test_real_step_lifecycle_against_fake_server(self):
        server = _LoopbackFloodServer()
        input_queue = queue.Queue()
        output_queue = queue.Queue()
        cancel_queue = queue.Queue()
        config = {
            "catch_signals": [
                {"source": "tts_SoS", "target": "stream_start"},
                {"source": "tts_EoS", "target": "stream_end"},
            ],
            "pass_signals": [
                {"source": "tts_SoS", "target": "item_SoS"},
                {"source": "tts_EoS", "target": "item_EoS"},
            ],
            "emit_signals": [],
            "input_vars": [{"source": "audio", "target": "audio"}],
            "pass_vars": [
                {"source": "action_hint", "target": "motion_hint"}
            ],
            "output_vars": [{"source": "motion", "target": "motion"}],
            "next_nodes": [-1],
            "ws_url": server.url,
            "model": "flood",
            "framerate": 30,
            "chunk_duration_ms": 200,
            "humanoid_output": False,
            "receive_timeout": 2,
            "poll_interval": 0.02,
        }
        step = MotionChunkGenerationStep(
            1,
            "test",
            logging.getLogger("test.MotionChunkGenerationStep"),
            queue.Queue(),
            input_queue,
            output_queue,
            cancel_queue,
            config,
        )
        thread = None
        try:
            self.assertIsNone(step.init_error)

            def wait_snapshot(expected):
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    if server.snapshot() == expected:
                        return
                    time.sleep(0.01)
                self.assertEqual(server.snapshot(), expected)

            # Pipeline init generates a real one-second probe (five 200ms
            # chunks), then releases the exclusive connection.
            wait_snapshot((1, 1, 0))
            self.assertEqual(
                [message["type"] for message in server.protocols[0].messages],
                ["start", "continue", "continue", "continue", "continue"],
            )
            self.assertEqual(
                [message["text"] for message in server.protocols[0].messages],
                ["test"] * 5,
            )

            thread = threading.Thread(target=step.run)
            thread.start()

            input_queue.put(json.dumps({
                "signal": "tts_SoS",
                "timestamp": 1,
                "pass_data": {
                    "action_hint": "walk naturally",
                    "item_id": "item-1",
                },
            }))
            start = json.loads(output_queue.get(timeout=2))
            wait_snapshot((2, 1, 1))

            input_queue.put(json.dumps({
                "timestamp": 1, "audio": "trigger"
            }))
            # Pipelined session: the reply emits on a later poll or at the
            # stream_end drain — either way before the relayed envelope end.
            input_queue.put(json.dumps({
                "signal": "tts_EoS", "timestamp": 1
            }))
            chunk = json.loads(output_queue.get(timeout=2))
            end = json.loads(output_queue.get(timeout=2))
            wait_snapshot((2, 2, 0))
            self.assertEqual(start["signal"], "item_SoS")
            self.assertEqual(start["pass_data"]["item_id"], "item-1")
            self.assertEqual(end["signal"], "item_EoS")
            self.assertEqual(_frame_ids(chunk), list(range(1, 7)))
            self.assertEqual(
                [message["text"] for message in server.protocols[1].messages],
                ["walk naturally"],
            )
        finally:
            cancel_queue.put(json.dumps({"signal": "kill", "timestamp": 9}))
            if thread is not None:
                thread.join(timeout=3)
                self.assertFalse(thread.is_alive())
            else:
                step.dispose()
            # close handshake / handler finalization is asynchronous
            time.sleep(0.05)
            server.close()


if __name__ == "__main__":
    unittest.main()
