# YACHIYO Server Progress

## 端口配置

| 服务            | 端口  |
| --------------- | ----- |
| vLLM (LLM)      | 8000  |
| YACHIYO 主服务器 | 8910  |
| WebRTC          | 15168 |
| ASR             | 8010  |
| TTS             | 8011  |
| Database        | 8100  |

## 环境配置

| 服务     | Conda 环境 | Python | 包版本                            |
| -------- | ---------- | ------ | --------------------------------- |
| vLLM     | vllm       | 3.12   | vllm==0.18.0, torch==2.10.0+cu128 |
| ASR      | qwen-asr   | 3.11   | qwen-asr (vLLM backend)           |
| TTS      | qwen-tts   | 3.10   | faster-qwen3-tts                  |
| Database | database   | 3.10   | sentence-transformers, faiss      |
| YACHIYO   | yachiyo     | 3.12   | uvicorn, fastapi                  |

## VRAM 配置

**所有服务同时运行（RTX 5090 32GB）：**

| Config               | DB       | ASR 0.6B (0.15) | TTS 0.6B | LLM       | Total     |
| -------------------- | -------- | --------------- | -------- | --------- | --------- |
| 9B (gpu_mem=0.5)     | 2778 MiB | 5798 MiB        | 3198 MiB | 15880 MiB | 27680 MiB |
| 4B (gpu_mem=0.35)    | 2806 MiB | 5798 MiB        | 3398 MiB | 11512 MiB | 23540 MiB |

注：9B 必须先启动 vLLM（profiling 峰值 ~19 GiB），再启动其他服务。

## vLLM 关键结论

- **gpu_mem 最低要求**：5090 + 0.18.0 使用 Triton/FLA GDN kernel（non-torch 3.81 GiB），gpu_mem 最低 0.35（4B）或 0.5（9B）
- **Prefix caching 无效**：Qwen3.5 GDN 层不支持 prefix caching，命中率 ~0%
- **ASR 0.6B**：纯 Transformer，不受 GDN 问题影响，gpu_mem=0.15 可行
- **TTS 并发**：faster-qwen3-tts CUDA Graph 不支持并发，需 threading.Lock 串行化

## Pipeline Benchmark（9B + TTS 0.6B，RTX 5090）

| Pipeline | ASR    | LLM total | TTS 1st   | E2E FA         | E2E Total   |
| -------- | ------ | --------- | --------- | -------------- | ----------- |
| Standard | 28±1ms | 290±88ms  | 875±323ms | **1139±347ms** | 2228±754ms  |
| SMPL     | 28±0ms | 489±106ms | 841±181ms | **1276±65ms**  | 3851±1150ms |

Multi-user（1→3）：first audio 从 966ms 增至 2878ms，每增 1 用户 ~950ms（TTS 串行瓶颈）。

## aiortc 帧交付核心结论

1. **jitter buffer 工作正常**：±50ms 抖动下零丢帧，PTS 保持连续
2. **丢帧无补偿**：无 PLC / 舒适噪声 / 冻结帧，PTS 跳跃标记缺口
3. **单路丢帧不影响另一路**
4. **A-V 同步**：baseline ~67ms 偏差（各自 jitter buffer 延迟），无跨流同步
5. **PTS 是唯一可靠的帧连续性判据**
6. **recv() 无内置超时**，完全丢失时永久阻塞，必须用 `asyncio.wait_for`

## WebRTC 实现要点

- 输入侧：PTS-based group assembler，audio gap 补静音，video gap 补上一帧，data 按到达顺序
- 输出侧：GroupDispatcher + consumer_offset（5ms）解决 asyncio 调度竞态
- FrameSplitter：clock-driven tick loop，connection_start 触发启动
- 帧率/分辨率全 session 可配，GCD 自动计算 group 结构
- 测试通过：320x240 ~ 1080p，15/24/30/60fps

## LLM 角色扮演

- 配置格式：input_vars/pass_vars/output_vars 统一为 `"source"/"target"` 单值
- Lorebook 三类分离：universal_rules、custom_rules/custom_reminder、character
- Tool call：天气查询（wttr.in）、网页搜索（ddgs），最后一轮 tool_choice="none"
- 变量替换：`{{time}}`/`{{date}}`/`{{weekday}}` 动态注入
- Prompt 优化：164 题测试全通过（110 基础 + 26 多轮 + 28 边缘）

## VTuber 弹幕 Pipeline

- DanmakuBuffer：优先级队列 + playback 背压 + idle 主动对话 + 弹幕合并去重
- 模型：Kimi-K2.5（远端 vLLM），首 token ~0.5s
- TTS 并发修复：threading.Lock 串行化

## TODO

- [ ] **记忆/摘要系统**：LLM 模块内部实现，参考 Qvink 逐条总结方案
- [ ] **Token 计数截断**：history_length 改为按 token 总数
- [ ] **长时间运行稳定性**：uvicorn 跑 1.5 天后无响应，需排查
- [ ] **钓鱼弹幕防御**：Kimi-K2.5 大部分能识别，偶尔仍被骗
- [ ] **SC 复述**：偶尔不先读 SC 内容
- [ ] **仅音频流支持**：当前 group assembler 要求 audio+video 都有数据才启动
