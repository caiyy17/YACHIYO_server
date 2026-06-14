import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from Modules.llm_utils.GesturePlan import (
    add_gesture_compat_fields,
    parse_gestures_tag,
)


class GesturePlanCompatTest(unittest.TestCase):
    def test_hidden_gestures_are_stripped_and_validated(self):
        visible, plan = parse_gestures_tag(
            '我把步骤记下来。第一步先确认输入。\n'
            '[gestures: [{"action":"write","sentence_index":0,'
            '"start_ratio":0.18,"end_ratio":0.82}]]'
        )

        self.assertEqual(visible, "我把步骤记下来。第一步先确认输入。")
        self.assertEqual(
            plan,
            [
                {
                    "action": "write",
                    "label": "写字",
                    "sentence_index": 0,
                    "start_ratio": 0.18,
                    "end_ratio": 0.82,
                }
            ],
        )

    def test_gesture_mode_keeps_legacy_llm_fields(self):
        gesture_plan = [
            {
                "action": "write",
                "label": "写字",
                "sentence_index": 0,
                "start_ratio": 0.18,
                "end_ratio": 0.82,
            }
        ]
        chunks = [
            {
                "text": "我把步骤记下来。",
                "raw_text": "我把步骤记下来。",
                "sentence_index": 0,
                "gesture_plan": gesture_plan,
            },
            {
                "text": "第一步先确认输入。",
                "raw_text": "第一步先确认输入。",
                "sentence_index": 1,
                "gesture_plan": gesture_plan,
            },
        ]
        for chunk in chunks:
            add_gesture_compat_fields(
                chunk,
                gesture_plan,
                chunk["sentence_index"],
                default_expression="默认",
            )

        self.assertEqual(len(chunks), 2)
        self.assertEqual(chunks[0]["text"], "我把步骤记下来。")
        self.assertEqual(chunks[0]["raw_text"], "我把步骤记下来。")
        self.assertEqual(chunks[0]["sentence_index"], 0)
        self.assertEqual(chunks[0]["gesture_plan"][0]["action"], "write")
        self.assertEqual(chunks[0]["action"], "写字")
        self.assertEqual(chunks[0]["expression"], "默认")

        self.assertEqual(chunks[1]["text"], "第一步先确认输入。")
        self.assertEqual(chunks[1]["raw_text"], "第一步先确认输入。")
        self.assertEqual(chunks[1]["sentence_index"], 1)
        self.assertEqual(chunks[1]["action"], "")
        self.assertEqual(chunks[1]["expression"], "默认")


if __name__ == "__main__":
    unittest.main()
