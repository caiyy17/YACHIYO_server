"""
Qwen3-TTS OpenAI-compatible server using faster-qwen3-tts (CUDA Graph acceleration).
"""

import argparse
import io
import os
import soundfile as sf
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Optional

app = FastAPI()
model = None
ref_cache = {}  # voice_name -> {audio_path, text}
LANGUAGES = ["auto", "chinese", "english", "french", "german", "italian", "japanese", "korean", "portuguese", "russian", "spanish"]


class SpeechRequest(BaseModel):
    model: str = "tts"
    input: str
    voice: str = "default"
    language: Optional[str] = "auto"
    speed: Optional[float] = 1.0
    response_format: Optional[str] = "wav"


def get_available_voices():
    return list(ref_cache.keys())


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

    language = req.language or "auto"
    voice = req.voice
    # Fallback to first loaded reference voice if requested voice not found
    if voice not in ref_cache and ref_cache:
        fallback = list(ref_cache.keys())[0]
        print(f"Voice '{voice}' not found, falling back to '{fallback}'")
        voice = fallback

    try:
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

    # Warmup with reference voice
    if ref_cache:
        name = list(ref_cache.keys())[0]
        ref = ref_cache[name]
        print(f"Warmup with voice '{name}'...")
        model.generate_voice_clone(text="test", language="chinese", ref_audio=ref["audio_path"], ref_text=ref["text"])
        print("Warmup done")

    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)
