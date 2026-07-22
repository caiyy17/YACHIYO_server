"""Regression tests for VADServer's independent idle-session reaper."""

import asyncio
import importlib.util
import sys
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT / "Modules_standalone" / "VADServer" / "vad_server.py"
SPEC = importlib.util.spec_from_file_location("vad_server_under_test", SERVER_PATH)
vad_server = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = vad_server
SPEC.loader.exec_module(vad_server)


class SessionReaperTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        with vad_server._lock:
            vad_server._sessions.clear()
        self.original_interval = vad_server.SESSION_REAP_INTERVAL_S
        self.original_ttl = vad_server.SESSION_TTL_S

    def tearDown(self):
        vad_server.SESSION_REAP_INTERVAL_S = self.original_interval
        vad_server.SESSION_TTL_S = self.original_ttl
        with vad_server._lock:
            vad_server._sessions.clear()

    async def test_stale_session_is_reaped_without_another_create(self):
        vad_server.SESSION_REAP_INTERVAL_S = 0.01
        vad_server.SESSION_TTL_S = 10
        now = time.monotonic()
        with vad_server._lock:
            vad_server._sessions["stale"] = [object(), now - 11]
            vad_server._sessions["active"] = [object(), now]

        async with vad_server.lifespan(None):
            await asyncio.sleep(0.03)

        with vad_server._lock:
            self.assertNotIn("stale", vad_server._sessions)
            self.assertIn("active", vad_server._sessions)

    async def test_append_activity_refreshes_ttl(self):
        vad_server.SESSION_REAP_INTERVAL_S = 0.005
        vad_server.SESSION_TTL_S = 0.04
        with vad_server._lock:
            vad_server._sessions["active"] = [object(), time.monotonic()]

        async with vad_server.lifespan(None):
            await asyncio.sleep(0.025)
            with vad_server._lock:
                vad_server._sessions["active"][1] = time.monotonic()
            await asyncio.sleep(0.025)
            with vad_server._lock:
                self.assertIn("active", vad_server._sessions)
            await asyncio.sleep(0.03)

        with vad_server._lock:
            self.assertNotIn("active", vad_server._sessions)


if __name__ == "__main__":
    unittest.main()
