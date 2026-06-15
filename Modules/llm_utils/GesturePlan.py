import json
import re


GESTURE_LABELS = {
    "left_scratch_head": "左手挠头",
    "left_cheek_on_hand": "左手托腮，手架在桌子上",
    "flip_book": "翻书",
    "head_tilt": "歪头",
    "write": "写字",
    "nod": "点头",
    "shake_head": "摇头",
    "think": "思考",
}

GESTURES_TAG_RE = re.compile(
    r"\n?\s*\[\s*gestures?\s*[:：]\s*(?P<body>\[.*\])\s*\]\s*$",
    re.IGNORECASE | re.DOTALL,
)


def parse_gestures_tag(raw, max_items=4):
    if not raw:
        return raw, []

    match = GESTURES_TAG_RE.search(raw)
    if not match:
        return raw, []

    cleaned = raw[: match.start()].rstrip()
    try:
        data = json.loads(match.group("body"))
    except Exception:
        return cleaned, []
    if not isinstance(data, list):
        return cleaned, []

    gestures = []
    for item in data[:max_items]:
        if not isinstance(item, dict):
            continue
        action = str(item.get("action") or "").strip()
        if action not in GESTURE_LABELS:
            continue
        try:
            sentence_index = int(item.get("sentence_index", 0))
            start_ratio = float(item.get("start_ratio", 0.0))
            end_ratio = float(item.get("end_ratio", 0.0))
        except (TypeError, ValueError):
            continue
        if sentence_index < 0:
            continue
        start_ratio = max(0.0, min(1.0, start_ratio))
        end_ratio = max(0.0, min(1.0, end_ratio))
        if end_ratio <= start_ratio:
            continue
        gestures.append(
            {
                "action": action,
                "label": GESTURE_LABELS[action],
                "sentence_index": sentence_index,
                "start_ratio": start_ratio,
                "end_ratio": end_ratio,
            }
        )
    return cleaned, gestures


def add_gesture_compat_fields(
    response,
    gesture_plan,
    sentence_index,
    default_expression="默认",
):
    gesture = None
    for item in gesture_plan:
        if item.get("sentence_index") == sentence_index:
            gesture = item
            break

    response.setdefault("action", "")
    response.setdefault("expression", default_expression)
    if gesture is not None:
        response["action"] = gesture.get("label", gesture.get("action", ""))
    return response
