"""
Qwen3-TTS OpenAI-compatible server using faster-qwen3-tts (CUDA Graph acceleration).
"""

import argparse
import base64
import io
import json
import os
import threading
import numpy as np
import soundfile as sf
from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel
from typing import Optional
from starlette.exceptions import HTTPException as StarletteHTTPException

app = FastAPI()


# OpenAI-shaped error body: {"error": {message, type, param, code}} so that
# openai SDK clients surface proper error messages (FastAPI default is {"detail"}).
@app.exception_handler(StarletteHTTPException)
async def _openai_http_error(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": {
            "message": str(exc.detail),
            "type": "invalid_request_error" if exc.status_code < 500 else "server_error",
            "param": None,
            "code": None,
        }},
    )


@app.exception_handler(RequestValidationError)
async def _openai_validation_error(request, exc):
    return JSONResponse(
        status_code=400,
        content={"error": {
            "message": str(exc.errors()),
            "type": "invalid_request_error",
            "param": None,
            "code": None,
        }},
    )
model = None
MODEL_SR = None  # native sample rate, captured at warmup (pcm streaming has no header)
ref_cache = {}  # voice_name -> {audio_path, text}
# CUDA Graph is not thread-safe — concurrent generate() calls corrupt the
# static KV cache and cause the decoder to miss EOS, producing abnormally
# long audio.  Serialize all inference with a lock.
# See: https://github.com/andimarafioti/faster-qwen3-tts/issues/85
_model_lock = threading.Lock()
LANGUAGES = ["auto", "chinese", "english", "french", "german", "italian", "japanese", "korean", "portuguese", "russian", "spanish"]


class SpeechRequest(BaseModel):
    model: str = "tts"
    input: str
    voice: str = "default"
    language: Optional[str] = "auto"
    speed: Optional[float] = 1.0
    response_format: Optional[str] = "wav"
    stream_format: Optional[str] = "audio"  # "audio" (binary body) | "sse" (event stream)
    duration: Optional[float] = None  # reference length (advisory, unused for now)


def get_available_voices():
    return list(ref_cache.keys())


def _to_pcm16_bytes(audio):
    """Float [-1,1] or int16 audio chunk -> raw 16-bit LE PCM bytes."""
    arr = np.asarray(audio).flatten()
    if arr.dtype != np.int16:
        arr = np.clip(arr.astype(np.float32), -1.0, 1.0)
        arr = (arr * 32767.0).astype(np.int16)
    return arr.tobytes()


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/v1/models")
def models_list():
    return {"object": "list", "data": [{"id": "tts", "object": "model"}]}

@app.get("/v1/tts/speakers")
def speakers():
    return {"speakers": get_available_voices()}

@app.get("/v1/tts/languages")
def languages():
    return {"languages": LANGUAGES}


@app.post("/v1/audio/speech")
def speech(req: SpeechRequest):
    if not req.input.strip():
        raise HTTPException(400, "Empty input")
    # Explicit 400 for unsupported knobs (no silent fallback to WAV)
    if req.stream_format not in ("audio", "sse"):
        raise HTTPException(
            400, f"stream_format '{req.stream_format}' not supported; use 'audio' or 'sse'"
        )
    if req.response_format not in ("wav", "pcm"):
        raise HTTPException(
            400,
            f"response_format '{req.response_format}' not supported by this server; "
            f"use 'wav' or 'pcm'",
        )

    language = req.language or "auto"
    voice = req.voice
    # Fallback to first loaded reference voice if requested voice not found
    if voice not in ref_cache and ref_cache:
        fallback = list(ref_cache.keys())[0]
        print(f"Voice '{voice}' not found, falling back to '{fallback}'")
        voice = fallback
    if voice not in ref_cache:
        raise HTTPException(500, "No reference voices loaded")

    # OpenAI-compatible SSE streaming (stream_format="sse"): text/event-stream of
    #   data: {"type": "speech.audio.delta", "audio": "<b64 pcm16>"}
    #   data: {"type": "speech.audio.done", "usage": {...}}
    # Audio payload is always raw PCM16 mono at the native sample rate (24kHz);
    # usage tokens are approximations (chars in / 12Hz codec steps out).
    if req.stream_format == "sse":
        ref = ref_cache[voice]

        def sse_stream():
            # Same locking rules as the binary pcm path (CUDA Graph is serial).
            with _model_lock:
                gen = model.generate_voice_clone_streaming(
                    text=req.input,
                    language=language,
                    ref_audio=ref["audio_path"],
                    ref_text=ref["text"],
                )
                total_samples = 0
                sr_seen = MODEL_SR or 24000
                try:
                    for chunk, sr, _timing in gen:
                        sr_seen = sr
                        pcm = _to_pcm16_bytes(chunk)
                        total_samples += len(pcm) // 2
                        event = {
                            "type": "speech.audio.delta",
                            "audio": base64.b64encode(pcm).decode("ascii"),
                        }
                        yield f"data: {json.dumps(event)}\n\n".encode()
                finally:
                    gen.close()
                usage = {
                    "input_tokens": len(req.input),
                    "output_tokens": int(total_samples / sr_seen * 12),
                }
                usage["total_tokens"] = usage["input_tokens"] + usage["output_tokens"]
                done = {"type": "speech.audio.done", "usage": usage}
                yield f"data: {json.dumps(done)}\n\n".encode()

        headers = {}
        if MODEL_SR:
            headers["X-Sample-Rate"] = str(MODEL_SR)
        return StreamingResponse(
            sse_stream(), media_type="text/event-stream", headers=headers
        )

    # OpenAI-compatible raw PCM streaming: 16-bit LE mono, chunks flushed as
    # generated (chunked transfer). Same as OpenAI, pcm carries no container
    # header; the sample rate is exposed via the X-Sample-Rate header.
    if req.response_format == "pcm":
        ref = ref_cache[voice]

        def pcm_stream():
            # CUDA Graph is not thread-safe: hold the lock for the whole
            # generation. Runs in a threadpool thread; acquire and release
            # happen in that same thread (incl. client-disconnect unwinding).
            with _model_lock:
                gen = model.generate_voice_clone_streaming(
                    text=req.input,
                    language=language,
                    ref_audio=ref["audio_path"],
                    ref_text=ref["text"],
                )
                try:
                    for chunk, sr, _timing in gen:
                        yield _to_pcm16_bytes(chunk)
                finally:
                    gen.close()

        headers = {}
        if MODEL_SR:
            headers["X-Sample-Rate"] = str(MODEL_SR)
        return StreamingResponse(
            pcm_stream(), media_type="application/octet-stream", headers=headers
        )

    try:
        with _model_lock:
            if voice in ref_cache:
                ref = ref_cache[voice]
                wavs, sr = model.generate_voice_clone(
                    text=req.input,
                    language=language,
                    ref_audio=ref["audio_path"],
                    ref_text=ref["text"],
                )
            else:
                raise HTTPException(500, "No reference voices loaded")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"TTS generation failed: {e}")

    buf = io.BytesIO()
    sf.write(buf, wavs[0], sr, format="WAV", subtype="PCM_16")
    buf.seek(0)
    return Response(content=buf.read(), media_type="audio/wav")


def load_reference_voices(ref_dir):
    if not os.path.isdir(ref_dir):
        return
    for fname in os.listdir(ref_dir):
        if not fname.endswith(".wav"):
            continue
        voice_name = fname[:-4]
        wav_path = os.path.join(ref_dir, fname)
        txt_path = os.path.join(ref_dir, voice_name + ".txt")
        if not os.path.exists(txt_path):
            print(f"Warning: no text file for {fname}, skipping")
            continue
        with open(txt_path, "r", encoding="utf-8") as f:
            ref_text = f.read().strip()
        ref_cache[voice_name] = {"audio_path": wav_path, "text": ref_text}
        print(f"Loaded reference voice: {voice_name} (text={ref_text[:30]}...)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    TTS_MODELS = {
        "0.6b": "Qwen/Qwen3-TTS-12Hz-0.6B-Base",
        "1.7b": "Qwen/Qwen3-TTS-12Hz-1.7B-Base",
    }
    parser.add_argument("--size", type=str, default="1.7b", choices=["0.6b", "1.7b"],
                        help="Model size: 0.6b or 1.7b")
    parser.add_argument("--checkpoint", type=str, default=None,
                        help="Model path (overrides --size)")
    parser.add_argument("--ref-dir", type=str, default="./voices")
    parser.add_argument("--device", type=str, default="cuda:0")
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8011)
    args = parser.parse_args()

    checkpoint = args.checkpoint or TTS_MODELS[args.size]

    from faster_qwen3_tts import FasterQwen3TTS

    print(f"Loading FasterQwen3TTS ({args.size}) from {checkpoint}...")
    model = FasterQwen3TTS.from_pretrained(checkpoint)
    print("Model loaded with CUDA Graph acceleration")

    load_reference_voices(args.ref_dir)
    print(f"Available voices: {get_available_voices()}")

    # Warmup with reference voice (also captures the native sample rate for pcm)
    if ref_cache:
        name = list(ref_cache.keys())[0]
        ref = ref_cache[name]
        print(f"Warmup with voice '{name}'...")
        _, MODEL_SR = model.generate_voice_clone(text="test", language="chinese", ref_audio=ref["audio_path"], ref_text=ref["text"])
        print(f"Warmup done (sample rate: {MODEL_SR})")

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)
