# YACHIYO Server

A real-time streaming pipeline server for embodied conversational agents. Orchestrates ASR, LLM, RAG, TTS, and motion generation as a linearized DAG pipeline with temporal consistency guarantees.

## Features

- **Linearized pipeline** — DAG of processing nodes executed as a sequential queue chain with formal temporal consistency (causal order, internal order, cancellation)
- **Intra-pipeline streaming** — LLM streams sentences incrementally to TTS; first audio output begins before full generation completes
- **Parallel execution** — dispatcher-receiver bracket enables concurrent processing of independent branches (e.g., TTS ∥ MotionGen)
- **WebSocket & WebRTC** — sentence-level streaming via WebSocket; frame-level (20ms) synchronized streaming via WebRTC with configurable fps/resolution
- **Service-oriented** — lightweight per-user pipeline instances; compute-heavy models run as shared standalone services with OpenAI-compatible APIs
- **Config-driven** — pipelines, models, and characters defined entirely in JSON; swap local/cloud backends by changing one config file

## Quick Start

```bash
conda activate yachiyo  # see requirements.txt for dependencies

# Pipeline server
uvicorn server_fastapi:app --host 0.0.0.0 --port 8910

# WebRTC server (optional, for frame-level streaming)
python server_webrtc.py --port 15168 --main-server http://localhost:8910
```

## Architecture

```
Client (WebSocket / WebRTC)
  │
  v
server_fastapi.py (port 8910)          Pipeline server
  │
  v
  Q_in -> [Node 1] -> Q_1 -> [Node 2] -> Q_2 -> ... -> Q_n -> send_queue -> Client
```

Each client gets an isolated pipeline instance (threads + queues). Compute-heavy models are shared standalone HTTP services.

## Model Services

| Service               | Directory                            | Description                                     | License                                                               |
| --------------------- | ------------------------------------ | ----------------------------------------------- | --------------------------------------------------------------------- |
| ASR (Qwen3-ASR)       | `Modules_standalone/QwenASR/`        | OpenAI Whisper-compatible wrapper for Qwen3-ASR | [Apache 2.0](https://github.com/QwenLM/Qwen3-ASR)                     |
| LLM (vLLM)            | `Modules_standalone/VLLM/`           | Config files for vLLM's native OpenAI API       | [Apache 2.0](https://github.com/vllm-project/vllm)                    |
| TTS (Qwen3-TTS)       | `Modules_standalone/QwenTTS/`        | OpenAI TTS-compatible wrapper for Qwen3-TTS     | [Apache 2.0](https://github.com/QwenLM/Qwen3-TTS)                     |
| MotionGen (HY-Motion) | `Modules_standalone/HYMotion/`       | REST API wrapper for text-to-motion generation  | [Hunyuan Community](https://github.com/Tencent-Hunyuan/HY-Motion-1.0) |
| Vector Database       | `Modules_standalone/VectorDatabase/` | BGE-M3 + FAISS similarity search server         | [MIT](https://huggingface.co/BAAI/bge-m3) / [MIT](https://github.com/facebookresearch/faiss) |

Each service runs in its own conda environment. Replace any service with any OpenAI-compatible implementation by editing `configs/settings/settings.json`.

## Pipeline Configurations

| Config              | Pipeline                                                     | Description                           |
| ------------------- | ------------------------------------------------------------ | ------------------------------------- |
| `demo`                | ASR → LLM → TTS                                                       | Minimal conversation                  |
| `unity_chan_default`  | ASR → LLM → DataQuery → DataQuery → TTS                                           | Conversation with RAG expression + action matching |
| `unity_chan_webrtc`   | AudioCollector → ASR → LLM → DataQuery → DataQuery → TTS → FrameSplitter          | WebRTC frame-level streaming          |
| `unity_chan_smpl`     | ASR → LLM → DataQuery → Dispatch → MotionGen ∥ TTS → Receive          | SMPLH motion generation (parallel)    |
| `unity_chan_live`     | DanmakuBuffer → LLM → DataQuery → Dispatch → MotionGen ∥ TTS → Receive | VTuber danmaku livestream             |

## Node Types

| Module                   | Function Name                       | Description                                                          |
| ------------------------ | ----------------------------------- | -------------------------------------------------------------------- |
| `webrtc_audio_collector` | `audio_collector`                   | Assembles WebRTC audio frames between vad_start/vad_end into WAV     |
| `asr_openai`             | `call_openai_asr`                   | Speech-to-text via OpenAI-compatible API                             |
| `llm_openai`             | `call_openai_llm`                   | Streaming LLM with history, lorebooks, tool calls, action extraction |
| `data_query_link`        | `call_data_query_link`              | RAG-based semantic matching via BGE embedding                        |
| `danmaku_buffer_vtuber`  | `call_danmaku_buffer_vtuber`        | Buffers and selects danmaku (live comments) for VTuber responses     |
| `motion_generation`      | `call_motion_generation`            | Text-to-motion via HY-Motion API; returns Unity humanoid motion by default (or raw SMPL-H) |
| `tts_openai`             | `call_openai_tts`                   | Text-to-speech via OpenAI-compatible API                             |
| `webrtc_frame_splitter`  | `frame_splitter`                    | Clock-driven output: splits TTS audio into synchronized frame groups |
| `parallel`               | `call_dispatcher` / `call_receiver` | Fork-join parallel execution bracket                                 |

## API

```
POST /register/                     Register client
POST /init_pipeline/{client_id}     Load pipeline config
WS   /ws/{client_id}                Connect WebSocket (send/receive JSON messages)
POST /unregister/                   Cleanup
```

WebRTC: `POST /offer/{client_id}` on port 15168 for SDP exchange, then communicate via audio/video tracks and DataChannel. Browser test client available at `http://<server>:15168/`.

## Web UI

```bash
cd webui && uvicorn web_ui:app --host 0.0.0.0 --port 8001
```

Open `http://localhost:8001` for client management, config viewing, and log monitoring. The visual pipeline editor is available at `http://localhost:8001/pipeline-editor`.

## Documentation

[Technical Report](technical_report/main.pdf) — architecture, formal proofs, latency analysis, pipeline config format, signal routing, timestamps, cancellation, and implementation details.

## License

[MIT](LICENSE). Standalone model services use their own licenses (see [Model Services](#model-services)).
