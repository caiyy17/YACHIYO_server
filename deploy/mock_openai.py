import io
import json
import math
import struct
import time
import wave

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse


app = FastAPI()

ACTION_RESPONSES = {
    "left_scratch_head": '这个点我有点拿不准。先让我把条件重新捋一下。\n[gestures: [{"action":"left_scratch_head","sentence_index":0,"start_ratio":0.18,"end_ratio":0.70}]]',
    "left_cheek_on_hand": '嗯，我先听着。这个问题可以慢慢想。\n[gestures: [{"action":"left_cheek_on_hand","sentence_index":0,"start_ratio":0.12,"end_ratio":0.78}]]',
    "flip_book": '我去翻一下资料。找到对应记录再告诉你。\n[gestures: [{"action":"flip_book","sentence_index":0,"start_ratio":0.10,"end_ratio":0.72}]]',
    "head_tilt": '诶，这个说法有点奇怪。你是指前一个版本吗？\n[gestures: [{"action":"head_tilt","sentence_index":0,"start_ratio":0.20,"end_ratio":0.66}]]',
    "write": '我把步骤记下来。第一步先确认输入，第二步再看输出。\n[gestures: [{"action":"write","sentence_index":0,"start_ratio":0.18,"end_ratio":0.82}]]',
    "nod": '对，这样理解是对的。后面就按这个接口走。\n[gestures: [{"action":"nod","sentence_index":0,"start_ratio":0.12,"end_ratio":0.58}]]',
    "shake_head": '不，这个不能直接这么接。否则动作会和语音错位。\n[gestures: [{"action":"shake_head","sentence_index":0,"start_ratio":0.10,"end_ratio":0.62}]]',
    "think": '让我想一下。这里最好按真实音频时长来算。\n[gestures: [{"action":"think","sentence_index":0,"start_ratio":0.18,"end_ratio":0.80}]]',
    "empty": "普通问候就不用动作。这样保持自然一点。\n[gestures: []]",
}


def choose_response(messages):
    prompt = ""
    if messages:
        prompt = messages[-1].get("content", "") or ""
    for action, response in ACTION_RESPONSES.items():
        if action in prompt:
            return response
    return ACTION_RESPONSES["empty"]


def make_wav(text):
    sample_rate = 24000
    duration = max(0.35, min(2.8, len(text) * 0.075))
    frames = int(sample_rate * duration)
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(sample_rate)
        for i in range(frames):
            sample = int(1200 * math.sin(2 * math.pi * 220 * i / sample_rate))
            wav.writeframes(struct.pack("<h", sample))
    return buf.getvalue()


@app.get("/v1/models")
async def models():
    return {"object": "list", "data": [{"id": "qwen", "object": "model"}]}


@app.post("/v1/chat/completions")
async def chat(request: Request):
    body = await request.json()
    content = choose_response(body.get("messages", []))
    if not body.get("stream", False):
        return JSONResponse(
            {
                "id": "mock-chat",
                "object": "chat.completion",
                "created": int(time.time()),
                "model": body.get("model", "qwen"),
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": content},
                        "finish_reason": "stop",
                    }
                ],
            }
        )

    async def events():
        for char in content:
            payload = {
                "id": "mock-chat",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": body.get("model", "qwen"),
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": char},
                        "finish_reason": None,
                    }
                ],
            }
            yield "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"
        payload = {
            "id": "mock-chat",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": body.get("model", "qwen"),
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }
        yield "data: " + json.dumps(payload, ensure_ascii=False) + "\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(events(), media_type="text/event-stream")


@app.post("/v1/audio/speech")
async def speech(request: Request):
    body = await request.json()
    wav = make_wav(body.get("input", ""))
    return Response(wav, media_type="audio/wav")
