# YACHIYO Server

实时流式 pipeline 服务器，用于具身对话代理。将 ASR、LLM、RAG、TTS 和动作生成编排为线性化 DAG pipeline，提供时序一致性保证。

## 特性

- **线性化 pipeline** — 处理节点的 DAG 执行为顺序队列链，形式化保证时序一致性（因果序、内部序、取消一致性）
- **pipeline 内流式** — LLM 逐句流式输出到 TTS，首段音频在完整生成前即开始播放
- **并行执行** — 分发-接收括号实现独立分支并发处理（如 TTS ∥ MotionGen）
- **WebSocket & WebRTC** — WebSocket 提供句子级流式；WebRTC 提供帧级（20ms）同步流式，帧率/分辨率可配
- **服务化架构** — 轻量的每用户 pipeline 实例；计算密集型模型作为共享独立服务运行(ASR/LLM/TTS 提供 OpenAI 兼容 API;无官方标准处用自定协议,如流式 VAD)
- **配置驱动** — pipeline、模型和角色完全由 JSON 定义；切换本地/云端只需改配置文件
- **声明式接口契约** — 节点的每个信号（catch / pass / emit）和每个输入/输出变量都以显式一对一 `{source, target}` 条目声明在 pipeline 配置中（双字段全写、含改名）；未声明的信号不会漂流穿过任何节点，转发副本与数据一样沿边逐跳。init 时按模块契约静态校验且双向恰好匹配：catch targets == 模块 required、emit 声明 == EMIT_SIGNALS、输入 targets == 模块声明的输入集、输出 sources == 其产物集。线上侧显式 `null` 为声明式退出：输入用默认值 / 输出不上线 / catch 不接线 / emit 不发射。控制面**事件**（如 `connection_start`、`playback_complete`）以同样格式声明——顶层 `events` 列表决定入口路由、节点 `catch_events` 条目声明消费——但经每个节点的 control queue 带外广播，不走数据路径（`cancel`/`kill` 为内建动词，不声明）

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

| 服务                  | 目录                                 | 说明                                     | 协议                                                                                         |
| --------------------- | ------------------------------------ | ---------------------------------------- | -------------------------------------------------------------------------------------------- |
| ASR (Qwen3-ASR)       | `Modules_standalone/QwenASR/`        | Qwen3-ASR 的 OpenAI Whisper 兼容 wrapper | [Apache 2.0](https://github.com/QwenLM/Qwen3-ASR)                                            |
| LLM (vLLM)            | `Modules_standalone/VLLM/`           | vLLM 原生 OpenAI API 的配置文件          | [Apache 2.0](https://github.com/vllm-project/vllm)                                           |
| TTS (Qwen3-TTS)       | `Modules_standalone/QwenTTS/`        | Qwen3-TTS 的 OpenAI TTS 兼容 wrapper     | [Apache 2.0](https://github.com/QwenLM/Qwen3-TTS)                                            |
| MotionGen (HY-Motion) | `Modules_standalone/HYMotion/`       | 文本到动作生成的 REST API wrapper        | [Hunyuan Community](https://github.com/Tencent-Hunyuan/HY-Motion-1.0)                        |
| 向量数据库            | `Modules_standalone/VectorDatabase/` | BGE-M3 + FAISS 相似度搜索服务            | [MIT](https://huggingface.co/BAAI/bge-m3) / [MIT](https://github.com/facebookresearch/faiss) |
| VAD                   | `Modules_standalone/VADServer/`      | 流式 VAD 会话(自定 HTTP 协议;Silero VAD 网络,另有 energy 轻量兜底) | — |

每个服务有独立的 conda 环境。模型服务为 OpenAI 兼容 API,替换实现只需编辑 `configs/settings/settings.json` 中的 HTTP 地址(VAD 服务为自定会话协议,替换需实现同一组端点)。

## Pipeline 配置

| 配置                  | Pipeline                                                                               | 说明                     |
| --------------------- | -------------------------------------------------------------------------------------- | ------------------------ |
| `demo`                | ASR → LLM → TTS                                                                        | 最小对话                 |
| `unity_chan_text`     | LLM → DataQuery → DataQuery                                                            | 纯文本对话（无音频）     |
| `unity_chan_default`  | ASR → LLM → DataQuery → DataQuery → TTS                                                | 对话 + RAG 表情/动作匹配 |
| `unity_chan_default_vad` | VAD → ASR → LLM → DataQuery → DataQuery → TTS                                       | WebSocket 上的服务端 VAD(自动打断)  |
| `unity_chan_webrtc`   | FrameCollector → VAD → ASR → LLM → DataQuery → DataQuery → TTS → Video → FrameSplitter | WebRTC 帧级流式传输      |
| `unity_chan_humanoid` | ASR → LLM → DataQuery → Dispatch → MotionGen ∥ TTS → Receive                           | Humanoid 动作生成（并行）   |
| `unity_chan_live`     | DanmakuBuffer → LLM → DataQuery → Dispatch → MotionGen ∥ TTS → Receive                 | VTuber 弹幕直播          |

## 节点类型

| 模块                     | 函数名                              | 说明                                                                           |
| ------------------------ | ----------------------------------- | ------------------------------------------------------------------------------ |
| `webrtc_frame_collector` | `frame_collector`                   | 逐组变换 WebRTC 车道:音频帧拼 WAV 块、视频/数据按 key 拆分                     |
| `vad_base`               | `call_vad`                          | 环形缓冲语音切段,由 recording_start/end 信号驱动(支持前后回溯、流式或整段输出) |
| `vad_server`             | `call_server_vad`                   | 模型驱动 VAD(经 VAD 服务):自动检测语音并 barge-in cancel;客户端信号可手动接管;`auto_detect: false` 时纯信号驱动 |
| `asr_openai`             | `call_openai_asr`                   | 通过 OpenAI 兼容 API 进行语音识别                                              |
| `llm_openai`             | `call_openai_llm`                   | 流式 LLM，支持历史记录、lorebook、工具调用、动作提取                           |
| `data_query_link`        | `call_data_query_link`              | 基于 BGE embedding 的 RAG 语义匹配                                             |
| `danmaku_buffer`         | `call_danmaku_buffer`               | 缓冲和筛选弹幕用于 VTuber 回复                                                 |
| `motion_generation`      | `call_motion_generation`            | 通过 HY-Motion API 生成动作；默认返回 Unity humanoid 格式（可选原始 SMPL-H）   |
| `tts_openai`             | `call_openai_tts`                   | 通过 OpenAI 兼容 API 进行语音合成                                              |
| `video_base`             | `call_video`                        | 占位视频生成:纯色帧(config `color`),片长由参考时长驱动                         |
| `pad`                    | `pad`                               | 同消息内各产物(音频 WAV + 帧列表)时长对齐:最长/最短/锚定三模式,每车道可关 cut/extend |
| `webrtc_frame_splitter`  | `frame_splitter`                    | 时钟驱动输出：将 TTS 音频拆分为同步帧组                                        |
| `parallel`               | `call_dispatcher` / `call_receiver` | 分发-接收并行执行括号                                                          |
| `parallel`               | `call_joint_stream`                 | 单节点内逐块合并 N 路 caller 流；整体长度可选 longest/shortest/anchor，每路可复制末块延长 |
| `memory_manager`         | `call_memory_manager`               | 观察者:经 SoS/EoS 跟踪 LLM 回复,把有实质内容的对话存入记忆                    |

仅 stream 路径使用 `exact_chunk`（默认 `true`，非 stream 完全不受影响）。
VAD/TTS 会为自然短尾补静音；Motion/Video 会复制最后一帧，直到短尾达到
`stream_frames`。设为 `false` 时保留短尾。请求时长始终保持原值。

## API

```
POST /register/                     注册客户端
POST /init_pipeline/{client_id}     加载 pipeline 配置（404 = 配置名不存在，400 = 配置校验失败，503 = 节点依赖服务 init 失败；均带明细）
WS   /ws/{client_id}                连接 WebSocket（收发 JSON 消息）
GET  /clients/{client_id}           客户端状态；初始化后附带 pipeline_config
POST /unregister/                   清理
```

WebRTC：在端口 15168 上 `POST /offer/{client_id}` 进行 SDP 交换，之后通过 audio/video track 和 DataChannel 通信。浏览器测试客户端：`http://<服务器>:15168/`。

WebRTC 会话的帧率参数（audio/video/data fps）写在 pipeline 配置里与 `pipeline` 平行的顶层 `webrtc` 段——网关在 offer 时经 `GET /clients/{client_id}` 读取（单一来源，与 FrameSplitter 的分组打包保持一致），且该段对 webrtc 类配置**必需**。offer 在应答前会对照管线校验：缺 `webrtc` 段、缺轨道或 DataChannel、`audio_fps` ≠ 50（线上固定 20ms Opus 帧）、video/data fps 不在支持列表内，均返回 400 并附具体缺口。视频分辨率由客户端自定（offer body 携带），网关将输出视频缩放到该尺寸。DataChannel 上，媒体走分组的 audio/video 车道，而每轮/每句的元数据（prompt、字幕文本、动作/表情）搭在信号的 `pass_data` 字段上；分组的 data 车道保留给帧对齐载荷。客户端消息的 `"direct": true` 标记（网关消费）是唯一的车道选择器：带标记的消息——信号也在内——经扣押 FIFO 以独立消息直入管线；不带标记的一律为 data 车道帧对齐载荷。例如把文本 prompt 打上 direct 直接定址到某个节点。stream TTS 接 WebRTC（纯配置，见 `dev_webrtc_stream`）按块流式发音频，每句带 `tts_SoS`/`tts_EoS` 包络。

## Web UI

```bash
cd webui && uvicorn web_ui:app --host 0.0.0.0 --port 8001
```

访问 `http://localhost:8001` 进行客户端管理、配置查看和日志监控。可视化 pipeline 编辑器位于 `http://localhost:8001/pipeline-editor`。

## 文档

[Technical Report](technical_report/main.pdf) — 架构设计、形式化证明、延迟分析、配置格式、信号路由、时间戳、取消机制及实现细节。

## 许可证

[MIT](LICENSE)。独立模型服务使用各自的许可证（见[模型服务](#模型服务)）。
