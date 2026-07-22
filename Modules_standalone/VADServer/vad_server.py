"""Streaming VAD service.

Custom session-based HTTP protocol; the field names (threshold /
prefix_padding_ms / silence_duration_ms, events speech_started /
speech_stopped) are the domain's generic terms:

  POST   /v1/audio/vad/sessions            create a session (detector config)
  POST   /v1/audio/vad/sessions/{id}/append   feed one b64 PCM16 mono chunk;
         returns the detector events fired inside that chunk, each with a
         negative offset in seconds relative to the END of the chunk
  DELETE /v1/audio/vad/sessions/{id}       drop a session
  GET    /health

Detectors are pluggable behind one interface: `silero` (the Silero VAD
network, per-session model instance) and `energy` (a lightweight RMS
threshold with debounce).
"""
import argparse
import asyncio
import base64
import threading
import time
import uuid
from contextlib import asynccontextmanager

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

FRAME_MS = 10                 # detector granularity
DEFAULT_THRESHOLD = 0.5       # detector-relative activation threshold
DEFAULT_SILENCE_MS = 300      # speech_stopped after this much silence
DEFAULT_PREFIX_MS = 0         # pre-roll is the pipeline vad's job
ACTIVATION_FRAMES = 3         # consecutive active frames to open speech
# energy detector: normalized RMS (0..1) that maps to threshold=0.5
ENERGY_RMS_AT_HALF = 0.04
# Idle sessions are reaped independently of session creation (covers a
# pipeline that died before it could send DELETE).
SESSION_TTL_S = 1800
SESSION_REAP_INTERVAL_S = 60


class EnergyDetector:
    """RMS-threshold stand-in detector: speech starts after
    ACTIVATION_FRAMES consecutive frames above threshold, stops after
    silence_duration_ms below it. Same event contract as a real network."""

    def __init__(self, threshold, silence_ms, sample_rate):
        self.rms_gate = ENERGY_RMS_AT_HALF * (threshold / 0.5)
        self.silence_frames = max(1, int(silence_ms / FRAME_MS))
        self.frame_samples = int(sample_rate * FRAME_MS / 1000)
        self.in_speech = False
        self.active_run = 0
        self.silent_run = 0
        self.buffer = np.zeros(0, dtype=np.int16)
        self.total_samples = 0    # samples fully consumed into frames
        self.fed = 0              # samples fed (consumed + buffered)
        self.sample_rate = sample_rate
        self.last_prob = 0.0

    def feed(self, pcm):
        """Consume one chunk; return detector events with absolute sample
        positions (in this session's stream)."""
        events = []
        self.fed += len(pcm)
        self.buffer = np.concatenate([self.buffer, pcm])
        while len(self.buffer) >= self.frame_samples:
            frame = self.buffer[:self.frame_samples]
            self.buffer = self.buffer[self.frame_samples:]
            self.total_samples += self.frame_samples
            rms = float(np.sqrt(np.mean(
                (frame.astype(np.float32) / 32768.0) ** 2)))
            self.last_prob = min(1.0, rms / (2 * self.rms_gate)) \
                if self.rms_gate > 0 else 0.0
            if rms >= self.rms_gate:
                self.active_run += 1
                self.silent_run = 0
                if not self.in_speech \
                        and self.active_run >= ACTIVATION_FRAMES:
                    self.in_speech = True
                    events.append(("speech_started", self.total_samples))
            else:
                self.silent_run += 1
                self.active_run = 0
                if self.in_speech and self.silent_run >= self.silence_frames:
                    self.in_speech = False
                    events.append(("speech_stopped", self.total_samples))
        return events


class SileroDetector:
    """Silero VAD (real network). The model runs at 16 kHz on 512-sample
    windows; session audio is decimated down to it and event positions are
    mapped back to the session sample rate. One model instance per session
    (the network keeps stream state inside the model object)."""

    def __init__(self, threshold, silence_ms, sample_rate):
        # heavy deps load lazily: energy-only deployments never import them
        import torch
        from silero_vad import load_silero_vad, VADIterator
        from scipy.signal import resample_poly
        if sample_rate % 16000 != 0:
            raise ValueError(
                f"silero requires a sample_rate that is a multiple of "
                f"16000, got {sample_rate} (48000 is the WebRTC standard; "
                f"44.1kHz-family clients must resample or use energy)")
        self._torch = torch
        self._resample = resample_poly
        self._down = sample_rate // 16000
        self.sample_rate = sample_rate
        self._iter = VADIterator(
            load_silero_vad(), threshold=threshold, sampling_rate=16000,
            min_silence_duration_ms=silence_ms, speech_pad_ms=0)
        self._buf16 = np.zeros(0, dtype=np.float32)
        self.fed = 0              # session-rate samples fed
        self.in_speech = False
        self.last_prob = 0.0

    def feed(self, pcm):
        events = []
        self.fed += len(pcm)
        x = pcm.astype(np.float32) / 32768.0
        if self._down > 1:
            x = self._resample(x, 1, self._down).astype(np.float32)
        self._buf16 = np.concatenate([self._buf16, x])
        while len(self._buf16) >= 512:
            win = self._buf16[:512]
            self._buf16 = self._buf16[512:]
            r = self._iter(self._torch.from_numpy(win))
            if r:
                if "start" in r:
                    self.in_speech = True
                    events.append(
                        ("speech_started", int(r["start"]) * self._down))
                if "end" in r:
                    self.in_speech = False
                    events.append(
                        ("speech_stopped", int(r["end"]) * self._down))
        self.last_prob = 1.0 if self.in_speech else 0.0
        return events


DETECTORS = {"energy": EnergyDetector, "silero": SileroDetector}

_sessions = {}   # sid -> [detector, last_used]
_lock = threading.Lock()


def _reap_stale_locked(now):
    for sid in [s for s, (_, ts) in _sessions.items()
                if now - ts > SESSION_TTL_S]:
        del _sessions[sid]


async def _session_reaper():
    """Remove idle sessions periodically, without waiting for a new create."""
    while True:
        await asyncio.sleep(SESSION_REAP_INTERVAL_S)
        with _lock:
            _reap_stale_locked(time.monotonic())


@asynccontextmanager
async def lifespan(_app):
    reaper = asyncio.create_task(
        _session_reaper(), name="vad-session-reaper"
    )
    try:
        yield
    finally:
        reaper.cancel()
        try:
            await reaper
        except asyncio.CancelledError:
            pass


app = FastAPI(lifespan=lifespan)


class SessionRequest(BaseModel):
    model: str = "energy"
    threshold: float = DEFAULT_THRESHOLD
    prefix_padding_ms: int = DEFAULT_PREFIX_MS   # accepted; pre-roll is client-side
    silence_duration_ms: int = DEFAULT_SILENCE_MS
    sample_rate: int = 48000


class AppendRequest(BaseModel):
    audio: str   # b64 raw PCM16 mono at the session's sample_rate


@app.get("/health")
def health():
    return {"status": "ok", "detectors": list(DETECTORS)}


@app.post("/v1/audio/vad/sessions")
def create_session(req: SessionRequest):
    if req.model not in DETECTORS:
        raise HTTPException(400, f"unknown model '{req.model}'")
    sid = uuid.uuid4().hex
    try:
        det = DETECTORS[req.model](
            req.threshold, req.silence_duration_ms, req.sample_rate)
    except Exception as e:
        raise HTTPException(400, f"detector init failed: {e}")
    now = time.monotonic()
    with _lock:
        _sessions[sid] = [det, now]
    return {"session_id": sid, "model": req.model}


@app.post("/v1/audio/vad/sessions/{sid}/append")
def append(sid: str, req: AppendRequest):
    with _lock:
        entry = _sessions.get(sid)
        if entry is not None:
            entry[1] = time.monotonic()
    if entry is None:
        raise HTTPException(404, "unknown session")
    det = entry[0]
    pcm = np.frombuffer(base64.b64decode(req.audio), dtype=np.int16)
    raw_events = det.feed(pcm)
    events = [
        {"type": etype,
         # negative seconds relative to the END of this chunk
         "offset_s": (sample - det.fed) / det.sample_rate}
        for etype, sample in raw_events
    ]
    return {"events": events, "probability": det.last_prob,
            "in_speech": det.in_speech}


@app.delete("/v1/audio/vad/sessions/{sid}")
def drop(sid: str):
    with _lock:
        _sessions.pop(sid, None)
    return {"status": "dropped"}


if __name__ == "__main__":
    import uvicorn
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8012)
    args = parser.parse_args()
    uvicorn.run(app, host="0.0.0.0", port=args.port)
