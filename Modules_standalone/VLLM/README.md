# vLLM LLM Server

[vLLM](https://github.com/vllm-project/vllm) natively serves an OpenAI-compatible API. No wrapper file needed — just a config yaml.

> **License**: vLLM is released under the [Apache 2.0 License](https://github.com/vllm-project/vllm/blob/main/LICENSE).

## Setup

```bash
conda create -n vllm python=3.12
conda activate vllm
pip install vllm
```

## Run

```bash
conda activate vllm
vllm serve <model_name> --config config_qwen.yaml
# Listens on port 5051
```

Example with Qwen3-8B-AWQ:

```bash
vllm serve Qwen/Qwen3-8B-AWQ --config config_qwen.yaml
```

## Config Files

- `config_qwen.yaml` — Qwen model config (port 5051, tool calling enabled)
- `config_glm.yaml` — GLM model config

## Configuration

Service address is configured in `configs/settings/settings.json`:

```json
{
    "llm": {
        "qwen_api": "http://127.0.0.1:5051/v1"
    }
}
```
