"""Opt-in probe for a real Flood Motion WebSocket service.

Run explicitly, never as part of offline regression:
  MOTION_WS_REAL_URL=http://host:18084 \
    python test/test_motion_websocket_real.py -v
"""

import os
import sys
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from Modules.motion_generation.MotionChunkGenerationStep import (  # noqa: E402
    MotionWebSocketSession,
)


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


REAL_URL = os.environ.get("MOTION_WS_REAL_URL")


@unittest.skipUnless(REAL_URL, "set MOTION_WS_REAL_URL for the real probe")
class RealMotionWebSocketTest(unittest.TestCase):
    def test_exact_chunks_continue_and_reset(self):
        session = MotionWebSocketSession(
            REAL_URL,
            6,
            30,
            logger=_Logger(),
            humanoid_output=False,
            receive_timeout=20,
        )
        try:
            session.reset({
                "motion_hint": "a person walks forward naturally"
            })
            chunks = [
                session.generate_chunk({}, index)["motion"]
                for index in range(3)
            ]
            self.assertEqual([len(chunk) for chunk in chunks], [6, 6, 6])
            self.assertEqual(chunks[0][0]["header"]["framerate"], 30)
            self.assertIn(
                chunks[0][0]["header"]["format"], {"smplh", "joints22"}
            )
            self.assertNotIn("header", chunks[1][0])

            session.finish()
            session.reset({"motion_hint": "a person waves one hand"})
            reset_chunk = session.generate_chunk({}, 0)["motion"]
            self.assertEqual(len(reset_chunk), 6)
            self.assertIn("header", reset_chunk[0])
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
