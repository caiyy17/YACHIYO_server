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

## Configuration

Service address is configured in `configs/settings/settings.json`:

```json
{
    "tts": {
        "qwen_tts_api": "http://127.0.0.1:8011/v1"
    }
}
```
