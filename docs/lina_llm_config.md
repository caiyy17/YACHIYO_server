# Lina LLM Config

`configs/unity_chan_default_lina.json` is the drop-in YACHIYO pipeline config for using Lina as the character LLM.

## What YACHIYO Owns

YACHIYO only handles:

- ASR input
- calling the Lina LLM endpoint
- TTS output
- converting Lina gesture ratios to audio-aligned timestamps
- WebSocket output fields

YACHIYO does not own Lina persona, action list, expression list, or character prompt in this config.

## Required Lina LLM Endpoint

The config expects Lina to expose an OpenAI-compatible chat completions endpoint:

```text
POST <lina_api>/chat/completions
```

In settings this is configured as:

```json
{
  "llm": {
    "lina_api": "https://YOUR_PUBLIC_LINA_LLM_HOST/v1"
  }
}
```

The model config is:

```json
{
  "api_base": "lina_api",
  "api_key": "LINA_API_KEY",
  "model_name": "lina"
}
```

## Response Contract

For YACHIYO's existing `call_openai_llm` step to parse gesture timestamps, Lina must return the assistant message content in this shape:

```text
正常可见回复正文。
[gestures: [{"action":"write","sentence_index":0,"start_ratio":0.18,"end_ratio":0.82}]]
```

If Lina internally stores `gesture_plan` as a structured field, the OpenAI-compatible wrapper should append it back to the returned `content` as the hidden trailing `[gestures: ...]` line. YACHIYO will strip that line before TTS/client output.

## How To Use

Use the config name:

```text
unity_chan_default_lina
```

Or copy it over `configs/unity_chan_default.json` if the caller is hardcoded to `unity_chan_default`.

Only these deployment settings should need changing:

```json
{
  "llm": {
    "lina_api": "https://YOUR_PUBLIC_LINA_LLM_HOST/v1"
  },
  "tts": {
    "qwen_tts_api": "http://127.0.0.1:8011/v1"
  },
  "asr": {
    "qwen_asr_api": "http://127.0.0.1:8010/v1"
  }
}
```
