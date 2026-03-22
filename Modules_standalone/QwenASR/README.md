# Qwen3-ASR Server

OpenAI-compatible ASR server wrapping [qwen-asr-serve](https://github.com/QwenLM/Qwen3-ASR) (vLLM backend with KV cache). Converts Qwen's output format to standard OpenAI verbose_json.

> **License**: Qwen3-ASR is released under the [Apache 2.0 License](https://github.com/QwenLM/Qwen3-ASR/blob/main/LICENSE). Please comply with its license terms.

## Setup

```bash
conda create -n qwen-asr python=3.11 -y
conda activate qwen-asr
pip install -U "qwen-asr[vllm]" httpx fastapi uvicorn python-multipart
```

## Run

```bash
conda activate qwen-asr
python qwen_asr_server.py
# Listens on port 8010
```

Use 0.6b model for lower VRAM:

```bash
python qwen_asr_server.py --size 0.6b
```

## API

Standard OpenAI `/v1/audio/transcriptions`:

```bash
curl http://localhost:8010/v1/audio/transcriptions \
  -F "file=@audio.wav" \
  -F "response_format=verbose_json"
```

Response:
```json
{"text": "识别出的文字", "language": "chinese", "duration": 2}
```

## Configuration

Service address is configured in `configs/settings/settings.json`:

```json
{
    "asr": {
        "qwen_asr_api": "http://127.0.0.1:8010/v1"
    }
}
```
