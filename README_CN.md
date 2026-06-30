# YACHIYO Server

实时流式 pipeline 服务器，用于具身对话代理。将 ASR、LLM、RAG、TTS 和动作生成编排为线性化 DAG pipeline，提供时序一致性保证。

## 特性

- **线性化 pipeline** — 处理节点的 DAG 执行为顺序队列链，形式化保证时序一致性（因果序、内部序、取消一致性）
- **pipeline 内流式** — LLM 逐句流式输出到 TTS，首段音频在完整生成前即开始播放
- **并行执行** — 分发-接收括号实现独立分支并发处理（如 TTS ∥ MotionGen）
- **WebSocket & WebRTC** — WebSocket 提供句子级流式；WebRTC 提供帧级（20ms）同步流式，帧率/分辨率可配
- **服务化架构** — 轻量的每用户 pipeline 实例；计算密集型模型作为共享独立服务运行，提供 OpenAI 兼容 API
- **配置驱动** — pipeline、模型和角色完全由 JSON 定义；切换本地/云端只需改配置文件

## 快速开始

```bash
conda activate yachiyo  # 依赖见 requirements.txt

# Pipeline 服务器
uvicorn server_fastapi:app --host 0.0.0.0 --port 8910

# WebRTC 服务器（可选，用于帧级流式）
python server_webrtc.py --port 15168 --main-server http://localhost:8910
```

## 架构

```
客户端 (WebSocket / WebRTC)
  │
  v
server_fastapi.py（端口 8910）          Pipeline 服务器
  │
  v
  Q_in -> [节点 1] -> Q_1 -> [节点 2] -> Q_2 -> ... -> Q_n -> send_queue -> 客户端
```

每个客户端拥有独立的 pipeline 实例（线程 + 队列）。计算密集型模型作为共享的独立 HTTP 服务运行。

## 模型服务

| 服务                  | 目录                                 | 说明                                     | 协议                                                                  |
| --------------------- | ------------------------------------ | ---------------------------------------- | --------------------------------------------------------------------- |
| ASR (Qwen3-ASR)       | `Modules_standalone/QwenASR/`        | Qwen3-ASR 的 OpenAI Whisper 兼容 wrapper | [Apache 2.0](https://github.com/QwenLM/Qwen3-ASR)                     |
| LLM (vLLM)            | `Modules_standalone/VLLM/`           | vLLM 原生 OpenAI API 的配置文件          | [Apache 2.0](https://github.com/vllm-project/vllm)                    |
| TTS (Qwen3-TTS)       | `Modules_standalone/QwenTTS/`        | Qwen3-TTS 的 OpenAI TTS 兼容 wrapper     | [Apache 2.0](https://github.com/QwenLM/Qwen3-TTS)                     |
| MotionGen (HY-Motion) | `Modules_standalone/HYMotion/`       | 文本到动作生成的 REST API wrapper        | [Hunyuan Community](https://github.com/Tencent-Hunyuan/HY-Motion-1.0) |
| 向量数据库            | `Modules_standalone/VectorDatabase/` | BGE-M3 + FAISS 相似度搜索服务            | [MIT](https://huggingface.co/BAAI/bge-m3) / [MIT](https://github.com/facebookresearch/faiss) |

每个服务有独立的 conda 环境。替换任何服务只需编辑 `configs/settings/settings.json` 中的 HTTP 地址。

## Pipeline 配置

| 配置                | Pipeline                                                     | 说明                   |
| ------------------- | ------------------------------------------------------------ | ---------------------- |
| `demo`                | ASR → LLM → TTS                                                       | 最小对话               |
| `unity_chan_default`  | ASR → LLM → DataQuery → DataQuery → TTS                                           | 对话 + RAG 表情/动作匹配    |
| `unity_chan_webrtc`   | AudioCollector → ASR → LLM → DataQuery → DataQuery → TTS → FrameSplitter          | WebRTC 帧级流式传输    |
| `unity_chan_smpl`     | ASR → LLM → DataQuery → Dispatch → MotionGen ∥ TTS → Receive          | SMPLH 动作生成（并行） |
| `unity_chan_live`     | DanmakuBuffer → LLM → DataQuery → Dispatch → MotionGen ∥ TTS → Receive | VTuber 弹幕直播        |

## 节点类型

| 模块                     | 函数名                              | 说明                                                 |
| ------------------------ | ----------------------------------- | ---------------------------------------------------- |
| `webrtc_audio_collector` | `audio_collector`                   | 在 vad_start/vad_end 之间收集 WebRTC 音频帧合成 WAV  |
| `asr_openai`             | `call_openai_asr`                   | 通过 OpenAI 兼容 API 进行语音识别                    |
| `llm_openai`             | `call_openai_llm`                   | 流式 LLM，支持历史记录、lorebook、工具调用、动作提取 |
| `data_query_link`        | `call_data_query_link`              | 基于 BGE embedding 的 RAG 语义匹配                   |
| `danmaku_buffer_vtuber`  | `call_danmaku_buffer_vtuber`        | 缓冲和筛选弹幕用于 VTuber 回复                       |
| `motion_generation`      | `call_motion_generation`            | 通过 HY-Motion API 生成动作；默认返回 Unity humanoid 格式（可选原始 SMPL-H） |
| `tts_openai`             | `call_openai_tts`                   | 通过 OpenAI 兼容 API 进行语音合成                    |
| `webrtc_frame_splitter`  | `frame_splitter`                    | 时钟驱动输出：将 TTS 音频拆分为同步帧组              |
| `parallel`               | `call_dispatcher` / `call_receiver` | 分发-接收并行执行括号                                |

## API

```
POST /register/                     注册客户端
POST /init_pipeline/{client_id}     加载 pipeline 配置
WS   /ws/{client_id}                连接 WebSocket（收发 JSON 消息）
POST /unregister/                   清理
```

WebRTC：在端口 15168 上 `POST /offer/{client_id}` 进行 SDP 交换，之后通过 audio/video track 和 DataChannel 通信。浏览器测试客户端：`http://<服务器>:15168/`。

## Web UI

```bash
cd webui && uvicorn web_ui:app --host 0.0.0.0 --port 8001
```

访问 `http://localhost:8001` 进行客户端管理、配置查看和日志监控。可视化 pipeline 编辑器位于 `http://localhost:8001/pipeline-editor`。

## 文档

[Technical Report](technical_report/main.pdf) — 架构设计、形式化证明、延迟分析、配置格式、信号路由、时间戳、取消机制及实现细节。

## 许可证

[MIT](LICENSE)。独立模型服务使用各自的许可证（见[模型服务](#模型服务)）。
