# YACHIYO Server Progress

## 近期改动

- **memory 模块通用化**：`memory_manager_vtuber` → `memory_manager`，注册名 `call_memory_manager`，落盘 `history/memory_{id}.json`。只写不读的回复摘要记录器，**当前未接入任何 config**。记录的 `timestamp` 改用**对话时间戳**（EoS 消息携带），非服务器墙钟。
- **danmaku 模块去 vtuber 后缀**：`danmaku_buffer_vtuber` → `danmaku_buffer`，注册名 `call_danmaku_buffer`。同步更新 `unity_chan_live.json`、webui 编辑器、README（中英）。
- **LLM prompt 回显**：`OpenaiStep` 在 **SoS 之后**经 `destination=-1` 把本轮输入 prompt 直达客户端（中继过所有下游节点、都不处理，最终进 send_queue）。字段名走 `add_output`+`output_vars` 可 rename；**所有 `call_openai_llm` 的 config（9 个）LLM 节点均已加** `{"output_name":"prompt","target":"prompt"}`。
- **DanmakuBuffer 输出时间戳统一**：批次释放与 idle 释放都用 `last_message_pts`（最后收到的消息时间戳），不再用 span 起点（第一条弹幕）。span 收集期的 cancel 锚点不变。
- **WebRTC 录音信号统一为 `recording_start`/`recording_end`**：客户端发的信号由 `vad_start`/`vad_end` 改名为 `recording_start`/`recording_end`（客户端已改）。`AudioCollectorStep` catch 这两个信号切分音频 span，并**重新发出同名信号**顺 pipeline 透传 → FrameSplitter 并入 → server_webrtc `_on_signal` 经 DataChannel 有序回客户端（server_webrtc 对信号是通用处理，无需改逻辑，仅注释）。`recording_end` 放在 WAV 前发，抢在 ASR 处理前到达；与 WAV 同轮时间戳。故客户端"发 recording_start → 收到 pipeline 有序回来的 recording_start"（同名往返，作为服务器权威确认）。

## 端口

| 服务            | 端口  |
| --------------- | ----- |
| vLLM (LLM)      | 8000  |
| YACHIYO 主服务器 | 8910  |
| WebRTC          | 15168 |
| ASR             | 8010  |
| TTS             | 8011  |
| Database        | 8100  |

## 环境

| 服务     | Conda 环境 | Python | 包版本                            |
| -------- | ---------- | ------ | --------------------------------- |
| vLLM     | vllm       | 3.12   | vllm==0.18.0, torch==2.10.0+cu128 |
| ASR      | qwen-asr   | 3.11   | qwen-asr (vLLM backend)           |
| TTS      | qwen-tts   | 3.10   | faster-qwen3-tts                  |
| Database | database   | 3.10   | sentence-transformers, faiss      |
| YACHIYO  | yachiyo    | 3.12   | uvicorn, fastapi                  |

## VRAM（RTX 5090 32GB，全服务并行）

| Config            | DB       | ASR (0.15) | TTS      | LLM       | Total     |
| ----------------- | -------- | ---------- | -------- | --------- | --------- |
| 9B (gpu_mem=0.5)  | 2778 MiB | 5798 MiB   | 3198 MiB | 15880 MiB | 27680 MiB |
| 4B (gpu_mem=0.35) | 2806 MiB | 5798 MiB   | 3398 MiB | 11512 MiB | 23540 MiB |

9B 须先启动 vLLM（profiling 峰值 ~19 GiB）再启其他服务。

## Pipeline Benchmark（9B + TTS 0.6B）

| Pipeline | ASR    | LLM total | TTS 1st   | E2E 首音       | E2E 总      |
| -------- | ------ | --------- | --------- | -------------- | ----------- |
| Standard | 28±1ms | 290±88ms  | 875±323ms | **1139±347ms** | 2228±754ms  |
| SMPL     | 28±0ms | 489±106ms | 841±181ms | **1276±65ms**  | 3851±1150ms |

多用户每增 1 人首音 +~950ms（TTS 串行瓶颈）。

## 关键结论 / 踩坑

- **vLLM**：5090 + 0.18.0 用 GDN kernel，gpu_mem 最低 0.35(4B)/0.5(9B)；Qwen3.5 GDN 不支持 prefix caching（命中 ~0）；ASR 0.6B 纯 Transformer，gpu_mem=0.15 可行。
- **TTS 并发**：faster-qwen3-tts CUDA Graph 不支持并发，需 `threading.Lock` 串行化。
- **aiortc**：jitter buffer 正常（±50ms 零丢帧）；丢帧无补偿，PTS 是唯一帧连续性判据；`recv()` 无超时须 `asyncio.wait_for`；A-V 无跨流同步（~67ms 偏差）。
- **WebRTC**：输入侧 PTS group assembler（audio 补静音 / video 补上帧）；输出侧 GroupDispatcher + consumer_offset(5ms)；FrameSplitter clock-driven tick；帧率/分辨率全 session 可配（已测 320x240~1080p，15/24/30/60fps）。

## Pipeline 路由机制

- 每节点默认发 `next_nodes[0]`；signal 不在 `catch_signal_set` 则自动透传，在则由 `process` 决定重发/吞。
- `SoS`/`EoS` 由 LLM 发；`destination=-1` 中继到末端出客户端、`-2` 直达下一个、`direct_send` 旁路直发（后两者当前无人用）。
- 并行靠 `DispatcherStep`（fan-out 到分支 + dispatch_start/end 跳给 Receiver）。

## LLM 角色扮演

- config：input_vars/pass_vars/output_vars 统一 `source/target` 单值。
- Lorebook 三类：universal_rules、custom_rules/reminder、character。
- Tool call：天气(wttr.in)、搜索(ddgs)，末轮 `tool_choice="none"`；变量 `{{time/date/weekday}}` 注入。
- Prompt 测试 164 题全通过。

## VTuber 弹幕

- DanmakuBuffer：优先级队列 + playback 背压 + idle 主动对话 + 合并去重。
- 模型 Kimi-K2.5（远端 vLLM，首 token ~0.5s）；TTS `threading.Lock` 串行化。

## TODO

- [ ] **记忆/摘要系统**：LLM 模块内部实现，参考 Qvink 逐条总结方案
- [ ] **Token 计数截断**：history_length 改为按 token 总数
- [ ] **长时间运行稳定性**：uvicorn 跑 1.5 天后无响应，需排查
- [ ] **钓鱼弹幕防御**：Kimi-K2.5 大部分能识别，偶尔仍被骗
- [ ] **SC 复述**：偶尔不先读 SC 内容
- [ ] **仅音频流支持**：当前 group assembler 要求 audio+video 都有数据才启动
