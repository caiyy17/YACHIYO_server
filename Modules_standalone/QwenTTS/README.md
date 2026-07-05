# Qwen3-TTS Server

OpenAI-compatible TTS server using [faster-qwen3-tts](https://github.com/andimarafioti/faster-qwen3-tts) (CUDA Graph acceleration). Voice clone only (Base model).

> **License**: [Qwen3-TTS](https://github.com/QwenLM/Qwen3-TTS) is released under the [Apache 2.0 License](https://github.com/QwenLM/Qwen3-TTS/blob/main/LICENSE). faster-qwen3-tts is released under the [MIT License](https://github.com/andimarafioti/faster-qwen3-tts/blob/main/LICENSE). Please comply with their respective licenses.

## Setup

```bash
conda create -n qwen-tts python=3.10 -y
conda activate qwen-tts
pip install -r requirements.txt
```

## Reference Voices

Place `.wav` + `.txt` pairs in `voices/` directory:

```
voices/
  default.wav    # reference audio
  default.txt    # transcript of the audio
```

## Run

```bash
conda activate qwen-tts
python qwen_tts_server.py --ref-dir ./voices
# Listens on port 8011
```

Use 0.6b model for lower VRAM:

```bash
python qwen_tts_server.py --size 0.6b --ref-dir ./voices
```

## API

Standard OpenAI `/v1/audio/speech`:

```bash
curl http://localhost:8011/v1/audio/speech \
  -H "Content-Type: application/json" \
  -d '{"input": "你好世界"}' \
  -o output.wav
```

Specify language:

```bash
curl http://localhost:8011/v1/audio/speech \
  -H "Content-Type: application/json" \
  -d '{"input": "こんにちは", "language": "japanese"}' \
  -o output.wav
```

PCM streaming (OpenAI-compatible): `response_format: "pcm"` streams raw 16-bit LE
mono PCM chunks as they are generated (24kHz, exposed via `X-Sample-Rate` header).
Consume with the OpenAI SDK:

```python
with client.audio.speech.with_streaming_response.create(
    model="tts", input=text, voice="test_cn", response_format="pcm",
) as resp:
    for chunk in resp.iter_bytes(65536):
        ...  # first chunk arrives in ~0.5s
```

SSE streaming (OpenAI-compatible): `stream_format: "sse"` returns `text/event-stream`
in the official event shape — `speech.audio.delta` events carrying base64 PCM16
chunks, terminated by one `speech.audio.done` with `usage` (tokens approximated:
input = chars, output = 12Hz codec steps):

```
data: {"type": "speech.audio.delta", "audio": "<base64 pcm16>"}
...
data: {"type": "speech.audio.done", "usage": {"input_tokens": 39, "output_tokens": 90, "total_tokens": 129}}
```

Supported `response_format`: `wav`, `pcm` — anything else returns an explicit 400
(no silent fallback). Errors use the OpenAI shape `{"error": {message, type, param,
code}}`, so openai SDK clients raise proper typed exceptions. `speed` and
`instructions` are accepted but ignored (not supported by the underlying model).

## Configuration

Service address is configured in `configs/settings/settings.json`:

```json
{
    "tts": {
        "qwen_tts_api": "http://127.0.0.1:8011/v1"
    }
}
```
