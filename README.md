# YACHIYO Server

A real-time streaming pipeline server for embodied conversational agents. Orchestrates ASR, LLM, RAG, TTS, and motion generation as a linearized DAG pipeline with temporal consistency guarantees.

## Features

- **Linearized pipeline** — DAG of processing nodes executed as a sequential queue chain with formal temporal consistency (causal order, internal order, cancellation)
- **Intra-pipeline streaming** — LLM streams sentences incrementally to TTS; first audio output begins before full generation completes
- **Parallel execution** — dispatcher-receiver bracket enables concurrent processing of independent branches (e.g., TTS ∥ MotionGen)
- **WebSocket & WebRTC** — sentence-level streaming via WebSocket; frame-level (20ms) synchronized streaming via WebRTC with configurable fps/resolution
- **Service-oriented** — lightweight per-user pipeline instances; compute-heavy models run as shared standalone services (OpenAI-compatible APIs for ASR/LLM/TTS; custom protocols where none exists, e.g. streaming VAD)
- **Config-driven** — pipelines, models, and characters defined entirely in JSON; swap local/cloud backends by changing one config file
- **Declared interface contracts** — every signal (catch/pass/emit) and every input/output var is declared in the pipeline config as explicit one-to-one `{source, target}` entries (both fields always written; renames included); undeclared signals never drift through a node, and relayed copies travel edge by edge like data. Per-node contracts are validated statically at init, exactly both ways: catch targets == the module's required catches, emit declarations == its EMIT_SIGNALS, input targets == its declared inputs, output sources == its products. An explicit `null` on the wire side is a declared opt-out: default input / unsent output / unwired catch / suppressed emission

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

| Service               | Directory                            | Description                                     | License                                                                                      |
| --------------------- | ------------------------------------ | ----------------------------------------------- | -------------------------------------------------------------------------------------------- |
| ASR (Qwen3-ASR)       | `Modules_standalone/QwenASR/`        | OpenAI Whisper-compatible wrapper for Qwen3-ASR | [Apache 2.0](https://github.com/QwenLM/Qwen3-ASR)                                            |
| LLM (vLLM)            | `Modules_standalone/VLLM/`           | Config files for vLLM's native OpenAI API       | [Apache 2.0](https://github.com/vllm-project/vllm)                                           |
| TTS (Qwen3-TTS)       | `Modules_standalone/QwenTTS/`        | OpenAI TTS-compatible wrapper for Qwen3-TTS     | [Apache 2.0](https://github.com/QwenLM/Qwen3-TTS)                                            |
| MotionGen (HY-Motion) | `Modules_standalone/HYMotion/`       | REST API wrapper for text-to-motion generation  | [Hunyuan Community](https://github.com/Tencent-Hunyuan/HY-Motion-1.0)                        |
| Vector Database       | `Modules_standalone/VectorDatabase/` | BGE-M3 + FAISS similarity search server         | [MIT](https://huggingface.co/BAAI/bge-m3) / [MIT](https://github.com/facebookresearch/faiss) |
| VAD                   | `Modules_standalone/VADServer/`      | Streaming VAD sessions (custom HTTP protocol; Silero VAD network, plus an energy fallback) | — |

Each service runs in its own conda environment. The model services expose OpenAI-compatible APIs and can be swapped for any compatible implementation by editing `configs/settings/settings.json` (the VAD service uses its own session protocol; swap it behind the same endpoints).

## Pipeline Configurations

| Config                | Pipeline                                                                               | Description                                        |
| --------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------- |
| `demo`                | ASR → LLM → TTS                                                                        | Minimal conversation                               |
| `unity_chan_text`     | LLM → DataQuery → DataQuery                                                            | Text-only conversation (no audio)                  |
| `unity_chan_default`  | ASR → LLM → DataQuery → DataQuery → TTS                                                | Conversation with RAG expression + action matching |
| `unity_chan_default_vad` | VAD → ASR → LLM → DataQuery → DataQuery → TTS                                       | Server-side VAD over WebSocket (auto barge-in)     |
| `unity_chan_webrtc`   | FrameCollector → VAD → ASR → LLM → DataQuery → DataQuery → TTS → Video → FrameSplitter | WebRTC frame-level streaming                       |
| `unity_chan_humanoid` | ASR → LLM → DataQuery → Dispatch → MotionGen ∥ TTS → Receive                           | Humanoid motion generation (parallel)                 |
| `unity_chan_live`     | DanmakuBuffer → LLM → DataQuery → Dispatch → MotionGen ∥ TTS → Receive                 | VTuber danmaku livestream                          |

## Node Types

| Module                   | Function Name                       | Description                                                                                                              |
| ------------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `webrtc_frame_collector` | `frame_collector`                   | Per-group transform of WebRTC lanes: audio frames→WAV chunk, video/data demux                                            |
| `vad_base`               | `call_vad`                          | Ring-buffered voice segmentation driven by recording_start/end signals (pre/post-roll, stream or whole-utterance output) |
| `vad_server`             | `call_server_vad`                   | Model-driven VAD via the VAD service: auto speech detection with barge-in cancel; client signals take manual control; `auto_detect: false` = purely signal-driven |
| `asr_openai`             | `call_openai_asr`                   | Speech-to-text via OpenAI-compatible API                                                                                 |
| `llm_openai`             | `call_openai_llm`                   | Streaming LLM with history, lorebooks, tool calls, action extraction                                                     |
| `data_query_link`        | `call_data_query_link`              | RAG-based semantic matching via BGE embedding                                                                            |
| `danmaku_buffer`         | `call_danmaku_buffer`               | Buffers and selects danmaku (live comments) for VTuber responses                                                         |
| `motion_generation`      | `call_motion_generation`            | Text-to-motion via HY-Motion API; returns Unity humanoid motion by default (or raw SMPL-H)                               |
| `tts_openai`             | `call_openai_tts`                   | Text-to-speech via OpenAI-compatible API                                                                                 |
| `video_base`             | `call_video`                        | Placeholder video generator: solid-color frames (config `color`), clip length driven by a reference duration             |
| `pad`                    | `pad`                               | Length-aligns the products in one message (audio WAV + frame lists) to the longest/shortest/anchor duration, per-lane cut/extend opt-outs |
| `webrtc_frame_splitter`  | `frame_splitter`                    | Clock-driven output: splits TTS audio into synchronized frame groups                                                     |
| `parallel`               | `call_dispatcher` / `call_receiver` | Fork-join parallel execution bracket                                                                                     |
| `parallel`               | `call_joint_stream`                 | Merges N caller streams chunk-by-chunk inside one node (e.g. TTS ∥ motion chunks packed per message)                     |
| `memory_manager`         | `call_memory_manager`               | Observer: tracks LLM responses via SoS/EoS and stores substantive conversations to memory                                |

## API

```
POST /register/                     Register client
POST /init_pipeline/{client_id}     Load pipeline config (404 = config name not found, 400 = config invalid, 503 = a node's dependent service failed at init; all with details)
WS   /ws/{client_id}                Connect WebSocket (send/receive JSON messages)
GET  /clients/{client_id}           Client status; includes pipeline_config once initialized
POST /unregister/                   Cleanup
```

WebRTC: `POST /offer/{client_id}` on port 15168 for SDP exchange, then communicate via audio/video tracks and DataChannel. Browser test client available at `http://<server>:15168/`.

WebRTC session timing (audio/video/data fps) lives in a top-level `webrtc` block in the pipeline config, parallel to `pipeline` — the gateway fetches it via `GET /clients/{client_id}` at offer time (single source, kept in sync with the FrameSplitter's group packing), and the block is mandatory for webrtc-facing configs. The offer is validated against the pipeline before answering: a missing `webrtc` block, missing tracks or DataChannel, `audio_fps` ≠ 50 (the wire is fixed at 20 ms Opus frames), or a video/data fps outside the supported divisor list is rejected with 400 and the concrete mismatches. Video resolution is the client's own choice, sent in the offer body; the gateway rescales outgoing video to it. On the DataChannel, media rides the group's audio/video lanes while per-turn/per-sentence metadata (prompt, subtitle text, action/expression) rides signals under a `pass_data` field; the group's data lane is reserved for frame-aligned payloads. Stream TTS over WebRTC (config-only, see `dev_webrtc_stream`) streams audio chunk-by-chunk with a per-sentence `tts_SoS`/`tts_EoS` envelope.

## Web UI

```bash
cd webui && uvicorn web_ui:app --host 0.0.0.0 --port 8001
```

Open `http://localhost:8001` for client management, config viewing, and log monitoring. The visual pipeline editor is available at `http://localhost:8001/pipeline-editor`.

## Documentation

[Technical Report](technical_report/main.pdf) — architecture, formal proofs, latency analysis, pipeline config format, signal routing, timestamps, cancellation, and implementation details.

## License

[MIT](LICENSE). Standalone model services use their own licenses (see [Model Services](#model-services)).
