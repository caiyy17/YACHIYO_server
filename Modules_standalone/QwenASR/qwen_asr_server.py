"""
Qwen3-ASR OpenAI-compatible server.
Starts qwen-asr-serve (vLLM backend with KV cache) as subprocess,
then serves a format-conversion proxy on the public port.
Single command, single process group.
"""

import argparse
import re
import subprocess
import time
import sys

import httpx
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional

app = FastAPI()
BACKEND_URL = ""
MODEL_NAME = ""


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/v1/models")
async def models():
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{BACKEND_URL}/v1/models")
        return r.json()


@app.post("/v1/audio/transcriptions")
async def transcriptions(
    file: UploadFile = File(...),
    model: str = Form(""),
    response_format: str = Form("verbose_json"),
    language: Optional[str] = Form(None),
):
    audio_data = await file.read()
    async with httpx.AsyncClient(timeout=60) as client:
        files = {"file": (file.filename or "audio.wav", audio_data, file.content_type or "audio/wav")}
        data = {"model": model or MODEL_NAME, "response_format": "json"}
        if language:
            data["language"] = language
        try:
            r = await client.post(f"{BACKEND_URL}/v1/audio/transcriptions", files=files, data=data)
        except Exception as e:
            raise HTTPException(502, f"Backend error: {e}")

    if r.status_code != 200:
        raise HTTPException(r.status_code, r.text)

    result = r.json()
    raw_text = result.get("text", "")

    # Parse: "language Chinese<asr_text>actual text" -> standard format
    match = re.match(r"language\s+(\w+)<asr_text>(.+)", raw_text, re.DOTALL)
    if match:
        lang = match.group(1).lower()
        text = match.group(2).strip()
    else:
        lang = "auto"
        text = raw_text.strip()

    if response_format == "verbose_json":
        return JSONResponse({"text": text, "language": lang, "duration": result.get("usage", {}).get("seconds", 0)})
    else:
        return JSONResponse({"text": text})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    ASR_MODELS = {
        "0.6b": "Qwen/Qwen3-ASR-0.6B",
        "1.7b": "Qwen/Qwen3-ASR-1.7B",
    }
    parser.add_argument("--size", type=str, default="1.7b", choices=["0.6b", "1.7b"],
                        help="Model size: 0.6b or 1.7b")
    parser.add_argument("--model", type=str, default=None,
                        help="Model path (overrides --size)")
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.3)
    parser.add_argument("--max-model-len", type=int, default=4096)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8010)
    parser.add_argument("--backend-port", type=int, default=5060)
    args = parser.parse_args()

    model_path = args.model or ASR_MODELS[args.size]
    BACKEND_URL = f"http://127.0.0.1:{args.backend_port}"

    # Start qwen-asr-serve as subprocess (vLLM with KV cache)
    print(f"Starting qwen-asr-serve ({args.size}) on internal port {args.backend_port}...")
    backend = subprocess.Popen([
        "qwen-asr-serve",
        model_path,
        "--gpu-memory-utilization", str(args.gpu_memory_utilization),
        "--max-model-len", str(args.max_model_len),
        "--host", "127.0.0.1",
        "--port", str(args.backend_port),
    ])

    # Wait for backend
    for i in range(60):
        time.sleep(2)
        try:
            r = httpx.get(f"{BACKEND_URL}/v1/models", timeout=2)
            if r.status_code == 200:
                data = r.json()
                MODEL_NAME = data["data"][0]["id"]
                print(f"Backend ready: {MODEL_NAME}")
                break
        except:
            pass
        if i % 5 == 0:
            print(f"  waiting... ({(i+1)*2}s)")
    else:
        print("Backend failed to start!")
        backend.kill()
        sys.exit(1)

    # Start proxy
    print(f"Starting proxy on {args.host}:{args.port} -> {BACKEND_URL}")
    import uvicorn
    try:
        uvicorn.run(app, host=args.host, port=args.port)
    finally:
        backend.kill()
