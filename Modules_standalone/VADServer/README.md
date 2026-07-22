# VAD Server

Streaming voice-activity-detection service. Custom session-based HTTP
protocol; the field names (`threshold`, `prefix_padding_ms`,
`silence_duration_ms`; events `speech_started` / `speech_stopped`) are
the domain's generic terms.

Detectors are pluggable behind one interface:

- `silero` — the Silero VAD network (16 kHz, 512-sample windows, one model
  instance per session). The default in the pipeline configs.
- `energy` — lightweight RMS threshold with debounce (no ML dependencies).

## Setup

```bash
conda create -n silerovad python=3.12 -y
conda activate silerovad
pip install fastapi uvicorn numpy
# for the silero detector:
pip install torch torchaudio --index-url https://download.pytorch.org/whl/cpu
pip install silero-vad scipy
```

## Run

```bash
conda activate silerovad
python vad_server.py --port 8012
```

## API

```
GET    /health
POST   /v1/audio/vad/sessions
         {"model": "energy", "threshold": 0.5,
          "silence_duration_ms": 300, "sample_rate": 48000}
       -> {"session_id": "..."}
POST   /v1/audio/vad/sessions/{id}/append
         {"audio": "<b64 raw PCM16 mono>"}
       -> {"events": [{"type": "speech_started", "offset_s": -0.12}],
           "probability": 0.8, "in_speech": true}
DELETE /v1/audio/vad/sessions/{id}
```

`offset_s` is negative, relative to the END of the appended chunk.
Successful pipeline teardown deletes its session immediately. As a fallback
for a caller that disappears without DELETE, the server also scans once per
minute and removes sessions that received no append for 30 minutes.
