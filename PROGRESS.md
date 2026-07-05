# YACHIYO Server Progress

## 近期改动

- **memory 模块通用化**：`memory_manager_vtuber` → `memory_manager`，注册名 `call_memory_manager`，落盘 `history/memory_{id}.json`。只写不读的回复摘要记录器，**当前未接入任何 config**。记录的 `timestamp` 改用**对话时间戳**（EoS 消息携带），非服务器墙钟。
- **danmaku 模块去 vtuber 后缀**：`danmaku_buffer_vtuber` → `danmaku_buffer`，注册名 `call_danmaku_buffer`。同步更新 `unity_chan_live.json`、webui 编辑器、README（中英）。
- **LLM prompt 回显**：`OpenaiStep` 在 **SoS 之后**经 `destination=-1` 把本轮输入 prompt 直达客户端（中继过所有下游节点、都不处理，最终进 send_queue）。字段名走 `add_output`+`output_vars` 可 rename；**所有 `call_openai_llm` 的 config（9 个）LLM 节点均已加** `{"output_name":"prompt","target":"prompt"}`。
- **DanmakuBuffer 输出时间戳统一**：批次释放与 idle 释放都用 `last_message_pts`（最后收到的消息时间戳），不再用 span 起点（第一条弹幕）。span 收集期的 cancel 锚点不变。
- **路由改革：`-1` 出口顶点化、哨兵退役**：`add_destination` 只做 `next_nodes[index]` 查表；`-1` 成为 next_nodes 合法取值（出口顶点），11 个 config 末节点 `[]`→`[-1]`，`-2` 与哨兵参数全部删除，4 处 `dest != -2` 判断简化。LLM 回显边显式进 config（`next_nodes: [x, -1]`，位置约定：第二条边即回显边）。editor 同步适配（extraNext 保持 + 无出线自动 `[-1]`）。**需重启 8910 生效。**
- **WebRTC 录音信号统一为 `recording_start`/`recording_end`**：客户端发的信号由 `vad_start`/`vad_end` 改名为 `recording_start`/`recording_end`（客户端已改）。`AudioCollectorStep` catch 这两个信号切分音频 span，并**重新发出同名信号**顺 pipeline 透传 → FrameSplitter 并入 → server_webrtc `_on_signal` 经 DataChannel 有序回客户端（server_webrtc 对信号是通用处理，无需改逻辑，仅注释）。`recording_end` 放在 WAV 前发，抢在 ASR 处理前到达；与 WAV 同轮时间戳。故客户端"发 recording_start → 收到 pipeline 有序回来的 recording_start"（同名往返，作为服务器权威确认）。网页测试端 `webrtc_client/index.html` 的 Hold-to-Talk 也已同步改名。

- **QwenTTS standalone 支持 OpenAI PCM 流式 + SSE 流式**：①`response_format:"pcm"` → StreamingResponse 逐块吐 16bit LE mono PCM（原生 24kHz=OpenAI pcm 规格，`X-Sample-Rate` 头）；②`stream_format:"sse"` → `text/event-stream`，官方事件形状 `speech.audio.delta`(base64 pcm16) + `speech.audio.done`(usage≈字符数/12Hz codec 步数)。底层 `generate_voice_clone_streaming`（chunk≈1s），`_model_lock` 罩全程、断连自动释放。**8012 测试实例全通过**：wav 兼容 / pcm 首块 0.49s / SSE 格式严格校验（8 delta+1 done、增量 0.24s 首事件）/ **三种模式 ASR 回环 similarity=1.000（逐字全对）** / 时长差 0.32s、RMS 差 5%（采样生成的正常抖动）/ **分块解码零边界伪影**（边界跳变 1688 < 全局 p99.9=6174）/ 断连恢复 / 并发串行。**错误契约对齐 OpenAI**：不支持的 `response_format`/`stream_format` 显式 400（不再静默回 WAV），错误体 `{"error":{message,type,param,code}}`（含 pydantic 422→400），SDK 抛带消息的 `BadRequestError`；错误路径 6 项 + 正常路径回归全过。**生产 8011 已替换为新代码**（PID 3723869，日志 `/tmp/qwen_tts_8011.log`），生产实测延迟：**随音频时长线性，~4× 实时**。管线典型短句（8-11 字，StreamCutter 粒度）wav 非流式 **347-447ms**（与原 benchmark TTS 1st 875ms 同量级，无退化）；长文本（39 字/7.5s 音频）1724-1889ms。流式首块 pcm **241ms** / SSE **272ms**（首块=12 codec steps≈1s 音频的生成量，与文本长度无关）——**短句收益 ~30%，长句/整段收益 ~87%**。坑：客户端窗口必须 ≤ 服务端单次 flush（`chunk_size×4000B`；chunk=12 → 48000B），超了会攒块等下一个 flush（65536 实测拖到 ~470ms）——**定为 32K**（48000B 下余量足，块数比 4K 少 8 倍利于将来块→管线消息）。**服务端 chunk_size 定为 12（库默认，不改）**：重放同 token 实验证明 chunk=6 首块省 118ms（269→151ms）但波形与 12 **不等价**（滑窗解码的可见历史随分块网格变化，局部差达峰值 17%；两者对整段解码真值偏差同量级 ~24%，即同精度的不同近似，感知无异、ASR 全对）——收益不值得引入"调度依赖的输出"，保持 12。实验脚本 `/tmp/test_chunk_equivalence.py`（monkeypatch token 重放法）。主服务 OpenaiTTSStep 仍用非流式（wav 路径不变，管线无感）。
- **recording_end 入口延迟 100ms**（`server_webrtc`，`RECORDING_END_HOLD_S=0.1`）：DataChannel 比音频轨快 ~80ms（localhost 实测，opus+jitter buffer 固有延迟，脚本 `/tmp/measure_av_dc_skew.py`），立即注入会在话尾音频落地前关掉 AudioCollector 的 span、切掉结尾。信号缓冲改为 `(due_time, raw)`，队头未到期整体等待（保 FIFO）；仅 recording_end 有 hold，其他信号即时。**根治方案（recording_end 带最后一帧 PTS、按媒体时间对齐）留作后续。**

## 端口

| 服务             | 端口  |
| ---------------- | ----- |
| vLLM (LLM)       | 8000  |
| YACHIYO 主服务器 | 8910  |
| WebRTC           | 15168 |
| ASR              | 8010  |
| TTS              | 8011  |
| Database         | 8100  |

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
- **`-1` = 出口顶点**：作为 `next_nodes` 的合法取值（不再是 add_destination 的哨兵参数），盖上 `destination=-1` 的消息被所有节点转发、从末节点流出到客户端（send_data 出口会剥掉 destination 字段）。**末节点必须写 `next_nodes: [-1]`**，空表/index 越界直接抛 `ValueError`（fail-fast，无静默兜底）。`-2` 已彻底退役；`direct_send` 仍无人用。
- LLM prompt 回显走位置约定（同 Dispatcher 风格）：`next_nodes = [主输出, 回显边?]`，接了第二条边就回显，没接就不回显——接线即开关，无额外配置键。WS 系 config 回显边 = `-1`（直达出口）；**webrtc 回显边 = `7`（frame_splitter）**：splitter 把"发给自己但无音频"的内容消息收进 `_pending_data`，装入下一个组的空 data 槽（常规即第 0 槽、通常是静音组），prompt 随 20Hz data 通道按序到客户端；cancel 清 pending + 槽装填时按时间戳丢弃过期项。
- 并行靠 `DispatcherStep`（fan-out 到分支 + dispatch_start/end 跳给 Receiver，全部具体节点号，不受影响）。
- webui editor 已适配：加载时把指向不存在节点的 next_nodes 条目（如 `-1`）存为 extraNext，保存时拼回；无出线节点导出自动补 `[-1]`。

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
