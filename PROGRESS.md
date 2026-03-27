# YACHIO Server Progress

## 端口配置

| 服务            | 端口  |
| --------------- | ----- |
| vLLM (LLM)      | 8000  |
| YACHIO 主服务器 | 8910  |
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
| YACHIO   | yachio     | 3.12   | uvicorn, fastapi                  |

## vLLM 版本问题调研

### 问题：4B + gpu_mem=0.3 在 5090 上无法正常工作

4090 上 vLLM 0.17.0 + Qwen3.5-4B-AWQ-4bit + gpu_memory_utilization=0.3 可以正常运行。
5090 上同样配置无论使用 0.17.x 或 0.18.0 都有问题。

### 0.17.x 的失败：Mamba CUDA Graph Assertion

**现象：**

```
vllm/model_executor/layers/mamba/ops/causal_conv1d.py:1162
assert num_cache_lines >= batch
AssertionError
```

**日志数据（0.17.1 + 4B + 0.3）：**

- Model loading: 3.85 GiB
- Available KV cache memory: 3.74 GiB（充足）
- 但 CUDA graph capture 时 assertion 失败

**根因（[vllm#34094](https://github.com/vllm-project/vllm/issues/34094)）：**

- CUDA graph capture size list 由 num_gpu_blocks 间接决定
- 5090（0.3×32GB=9.8GB）比 4090（0.3×24GB=7.2GB）分配更多 KV blocks
- 更多 blocks → capture list 延伸到更大的 batch size → 超过 Mamba conv_state cache lines
- 4090 因内存少，capture list 自然截断在安全范围内
- **不是 GPU 架构问题，是 vLLM 对高显存 GPU 的 bug**

**修复状态：** PR [#34571](https://github.com/vllm-project/vllm/pull/34571) 已合并到 vLLM 0.18.0

### 0.18.0 的失败：Profiling 内存过高

**现象：** `num_gpu_blocks=0`，Available KV cache memory 极低

**vLLM 0.18.0 + Python 3.10 的额外问题：**

- `AttributeError: standalone_compile does not have FakeTensorMode`
- 根因：Python 3.10 的 `mock.patch` 用 `getattr` 解析路径命中同名函数而非模块（[PR #37158](https://github.com/vllm-project/vllm/pull/37158)）
- **解决方案：使用 Python 3.12**（`conda create -n vllm python=3.12 && pip install vllm`）

**核心问题：Profiling 内存差异**

所有测试在 5090 干净 GPU 上单独运行，无其他服务占用。

| 配置                                    | vLLM   | gpu_mem | Available KV | 推理            |
| --------------------------------------- | ------ | ------- | ------------ | --------------- |
| 4B + vision                             | 0.17.1 | 0.3     | **3.74 GiB** | Mamba assertion |
| 4B + vision                             | 0.18.0 | 0.3     | **0.04 GiB** | 卡死            |
| 4B + vision + mode=0（无torch.compile） | 0.18.0 | 0.3     | **0.04 GiB** | 卡死            |
| 4B + image=0（跳过图片profiling）       | 0.18.0 | 0.3     | **0.46 GiB** | ✓               |
| 4B + language_model_only                | 0.18.0 | 0.3     | **1.06 GiB** | ✓               |
| 4B + vision                             | 0.18.0 | 0.35    | **1.61 GiB** | ✓               |
| 4B + vision                             | 0.18.0 | 0.4     | 充足         | ✓（12960 MiB）  |

**结论：**

- torch.compile **不是**主因（mode=0 结果相同）
- 视觉编码器 profiling 贡献 ~1.0 GiB 额外开销
- 0.18.0 模型 profiling 本身比 0.17.1 多用 ~2.7 GiB（原因未查明，见 TODO）
- 0.18.0 的 `num_gpu_blocks` 始终为 0 被 override 到 512，Available KV 是 override 后的剩余空间

**3.7 GiB 差距分解（debug 日志确认）：**

0.18.0 profiling 内存分解（`VLLM_LOGGING_LEVEL=DEBUG`）：

```
Total non KV cache memory: 8.05 GiB
├── weights memory: 3.23 GiB
├── torch peak memory increase: 1.01 GiB
└── non-torch forward increase: 3.81 GiB  ← 主因
```

- **~2.0 GiB：Triton/FLA GDN prefill kernel 变化**。0.18.0 使用新的 `Triton/FLA GDN prefill kernel` 实现，profiling 时 non-torch forward memory = 3.81 GiB（不被 PyTorch 追踪的 Triton 编译 CUDA kernel 内存）。0.17.1 使用旧的 GDN 实现，non-torch 约 1.8 GiB
- **~1.0 GiB：视觉编码器 profiling 变化**
- torch peak (activation) 两个版本接近（~1.0 GiB），不是主因
- torch.compile 从 v0.8.0 起就是 V1 引擎默认（[v0.8.0 release](https://github.com/vllm-project/vllm/releases/tag/v0.8.0)），0.17.x 也在用，不是 0.18.0 新引入
- mode=0 结果相同因为 non-torch 内存来自 Triton GDN kernel 本身，不受 compile mode 控制

### 0.18.0 Mamba Fix 确认生效

当 gpu_mem 足够时，Mamba fix 正常工作：

```
Capping cudagraph capture sizes from max 512 to 96 to fit Mamba cache blocks (99 blocks available)
```

### VRAM 实测数据

**vLLM 0.18.0 + Python 3.12，单独运行（无其他服务）：**

| 模型                     | gpu_mem | VRAM      |
| ------------------------ | ------- | --------- |
| 4B + language_model_only | 0.3     | 10044 MiB |
| 4B + vision              | 0.35    | 11250 MiB |
| 4B + vision              | 0.4     | 12960 MiB |
| 9B + vision              | 0.5     | 15880 MiB |

**vLLM 0.18.0 + Python 3.12，所有服务同时运行（yachio env 服务器，vLLM 先启动）：**

| Config               | DB       | ASR 0.6B (0.15) | TTS 0.6B | LLM       | Total     |
| -------------------- | -------- | --------------- | -------- | --------- | --------- |
| B: 9B (gpu_mem=0.5)  | 2778 MiB | 5798 MiB        | 3198 MiB | 15880 MiB | 27680 MiB |
| A: 4B (gpu_mem=0.35) | 2806 MiB | 5798 MiB        | 3398 MiB | 11512 MiB | 23540 MiB |

注：9B 必须先启动 vLLM（profiling 峰值 ~19 GiB），再启动其他服务。4B 无此限制。

### 完整 Pipeline Benchmark（vLLM 0.18.0，yachio 服务器 port 8910）

测试方法：`test/test_all_configs.py`，5 轮（首轮 warmup 排除），unity_chan / unity_chan_smpl config。

**Config B: 9B + TTS 0.6B（gpu_mem=0.5，27680 MiB）**

| Pipeline | ASR    | LLM total | TTS 1st   | Server FA  | E2E FA         | E2E Total   |
| -------- | ------ | --------- | --------- | ---------- | -------------- | ----------- |
| Standard | 28±1ms | 290±88ms  | 875±323ms | 1101±331ms | **1139±347ms** | 2228±754ms  |
| SMPL     | 28±0ms | 489±106ms | 841±181ms | 1128±191ms | **1276±65ms**  | 3851±1150ms |

**Config A: 4B + TTS 0.6B（gpu_mem=0.35，23540 MiB）**

| Pipeline | E2E First Audio | E2E Total  |
| -------- | --------------- | ---------- |
| Standard | **1495±779ms**  | 2952±406ms |
| SMPL     | **1176±25ms**   | 2470±909ms |

注：4B per-stage 日志解析间歇性失败，E2E 数据从 WebSocket 直接测量。

**与 Technical Report 原始数据对比（SenseVoice + 9B + BertVITS2）：**

| 指标               | TR 原始       | 0.18.0 9B Standard | 0.18.0 9B SMPL |
| ------------------ | ------------- | ------------------ | -------------- |
| Server first audio | **1060±22ms** | **1101±331ms**     | **1128±191ms** |
| E2E first audio    | 1101±32ms     | 1139±347ms         | 1276±65ms      |

### vLLM gpu_memory_utilization 机制

基于源码（`vllm/worker/worker.py`）和实测：

- `gpu_memory_utilization` 是**总显存**的比例
- KV cache block 数量在启动时固定
- CUDA graph 在 KV cache 预算之外额外分配 1-3GB（[vllm#14632](https://github.com/vllm-project/vllm/issues/14632)）
- 启动顺序**不影响**显存分配（实测 vLLM 先启动 vs 后启动，VRAM 相同）
- 运行中杀其他 GPU 服务后 PyTorch CUDA caching allocator 会占住释放的显存

### GDN Kernel 架构限制

Qwen3.5 的 GDN (Gated Delta Net) 层有三种 kernel 实现，按 GPU 架构和 vLLM 版本分配：

| GPU  | SM   | vLLM 0.17.x                 | vLLM 0.18.0                 | non-torch memory                |
| ---- | ---- | --------------------------- | --------------------------- | ------------------------------- |
| 4090 | 8.9  | 旧 C++ `gdn_attention_core` | Triton/FLA `forward_native` | 0.17: ~1.8 GiB / 0.18: 3.81 GiB |
| H200 | 9.0  | 旧 C++                      | FlashInfer `forward_cuda`   | ~1.8 GiB                        |
| 5090 | 12.0 | 旧 C++                      | Triton/FLA `forward_native` | 0.17: ~1.8 GiB / 0.18: 3.81 GiB |

**FlashInfer GDN kernel 仅支持 SM 9.0**——使用 Hopper 专有的 wgmma/TMA 指令（[flashinfer PR#2387](https://github.com/flashinfer-ai/flashinfer/pull/2387)）。SM 12.0 (Blackwell) 和 SM 8.9 (Ada) 都不支持这些指令，fallback 到 Triton/FLA。

**Triton/FLA kernel 的 non-torch 内存显著更高**（3.81 vs ~1.8 GiB），因为 Triton 编译的 CUDA kernel 通过 CUDA malloc 直接分配（不经过 PyTorch allocator）。

**对 gpu_memory_utilization 的影响：**

- H200 + 0.18.0：FlashInfer → non-KV 约 6 GiB → gpu_mem=0.3 可行
- 5090 + 0.18.0：Triton/FLA → non-KV 约 8-10 GiB → gpu_mem=0.3 不可行，最低 0.35
- 4090 + 0.17.0：旧 C++ → non-KV 约 6 GiB → gpu_mem=0.3 可行
- 4090 + 0.18.0：Triton/FLA → non-KV 约 8 GiB → gpu_mem=0.3 **也不可行**（7.2 GiB 预算 < 8 GiB non-KV）

**Blackwell 优化的 GDN kernel 正在开发中**（[flashinfer#2493](https://github.com/flashinfer-ai/flashinfer/issues/2493)，MLSys 2026 contest track），但未进入任何稳定版本。

**vLLM 已知相关 issue：**

- [vllm#36598](https://github.com/vllm-project/vllm/issues/36598)：Triton autotuner OOM on GDN layers (non-SM90 GPUs)，RTX 5090 明确列为受影响
- [vllm#35138](https://github.com/vllm-project/vllm/issues/35138)：FlashInfer accuracy issues on Blackwell (Qwen3.5)

### LLM 版本对比（9B，单独跑，干净 GPU，gpu_mem=0.5）

| 配置                   | VRAM      | Available KV | first_token | first_sentence | total (200 tokens) |
| ---------------------- | --------- | ------------ | ----------- | -------------- | ------------------ |
| 0.17.1 无 prefix cache | 16236 MiB | 5.26 GiB     | 20ms        | 35ms           | 1037ms             |
| 0.17.1 + prefix cache  | 16238 MiB | 5.26 GiB     | 20ms        | 35ms           | 1046ms             |
| 0.18.0 无 prefix cache | 15880 MiB | 1.56 GiB     | 20ms        | 35ms           | 1027ms             |
| 0.18.0 + prefix cache  | 15880 MiB | 1.56 GiB     | 21ms        | 36ms           | 1036ms             |

结论：

- 推理速度无差异（两版本、开关 prefix cache 均 ~1030-1046ms）
- Prefix caching 对 Qwen3.5 无效：GDN 层强制 `mamba_cache_mode='align'`，~0% 命中率
- 0.18.0 总 VRAM 更少（15880 vs 16236），但 Available KV 更少（1.56 vs 5.26 GiB），因为 Triton GDN kernel non-torch 内存更高
- 0.18.0 的 num_gpu_blocks=0 被 override 到 512（与 0.17.1 计算方式不同）
- 0.18.0 的 CUDA graph profiling 目的是**解决 startup OOM**（[PR #30515](https://github.com/vllm-project/vllm/pull/30515)，[RFC #27951](https://github.com/vllm-project/vllm/issues/27951)）：0.17.x 先分配 KV cache 再捕获 CUDA graph，高 gpu_mem 时两者合计可能超出 GPU → startup OOM。0.18.0 先测 CUDA graph 再定 KV cache → gpu_mem 不需要手动预留 headroom
- 两个版本 runtime 都是稳定的（CUDA graph 启动后固定，不会运行时增长）
- 0.18.0 profiling 阶段临时峰值（~19 GiB for 9B）超出 gpu_mem=0.5 的 15.68 GiB 目标。和其他服务共享 GPU 时必须先启动 vLLM（profiling 需要空 GPU），0.17.1 无此问题（profiling 峰值接近最终状态）
- PR #30515 解决了高 gpu_mem（如 0.9）下 KV+CUDA graph 合计超标的 startup OOM，但引入了 profiling 峰值超出 gpu_mem 目标的新问题，影响多服务 co-located 部署

### ASR on vLLM 0.18.0

Qwen3-ASR-0.6B 是纯 Transformer 模型（无 GDN/Mamba 层），non-torch forward memory 只有 0.2 GiB，不受 Triton GDN kernel 影响。

| 指标                               | 值                                                         |
| ---------------------------------- | ---------------------------------------------------------- |
| non-KV total                       | 2.81 GiB（weights 1.53 + torch_peak 1.09 + non_torch 0.2） |
| 最低 gpu_mem（max_model_len=4096） | 0.12（VRAM 4340 MiB）                                      |
| gpu_mem=0.15（max_model_len=4096） | 可行，约 4.7 GiB                                           |

注：`qwen_asr_server.py` 默认传 `--max-model-len 4096`，不指定则模型默认 65536 会 OOM。

### Prefix Caching

- Qwen3.5 的 GDN 层**不支持** prefix caching（[vllm#36493](https://github.com/vllm-project/vllm/issues/36493)）
- `--enable-prefix-caching` 可以开启，Attention 层的 KV cache 可以复用
- 但 GDN 层的 state 无法在 block 边界做 snapshot，必须从头重算
- 实测命中率接近 0%（[PR #36649](https://github.com/vllm-project/vllm/pull/36649) 待合并）

### TTS Voice Fallback

- TTS server 已修改：不存在的 voice name 自动 fallback 到 default voice 并打印 warning
- 两个项目（YACHIO + Mio）均已同步

### flash-attn Build Failure

- torch 2.10.0+cu128 与 prebuilt wheels 不兼容，source build 会卡死
- faster-qwen3-tts 使用 CUDA Graph 替代，不需要 flash-attn

### Technical Report 实测数据（0.6B ASR + 9B LLM + 0.6B TTS，RTX 5090，vLLM 0.18.0）

**Multi-User Scalability（test_multiuser_proper.py，unity_chan config，pipeline 预初始化）：**

| Users | FA avg (ms) | FA max (ms) | Total avg (ms) |
| ----- | ----------- | ----------- | -------------- |
| 1     | 966         | 966         | 1372           |
| 2     | 2070        | 2072        | —              |
| 3     | 2878        | 2883        | 5427           |
| 5     | 12234       | 26680       | 26691          |

1-3 用户性能与旧管线（SenseVoice+BertVITS2）持平。5 用户大幅退化（vLLM 0.18.0 KV cache 1.56 GiB，5 并发超出容量）。

**WebRTC Streaming（test_webrtc.py，unity_chan_webrtc config）：**

- Duration: 45.2s
- Audio frames: 2262 sent, 2259 received
- Video frames: 1357 sent, 1357 received
- DataChannel messages: 901
- ASR: "这是一段测试音频。" ✓

**Motion Generation SMPL（5 rounds, first=warmup, MotionGen at 10.81.7.113:7861）：**

| Config                              | First Audio   |
| ----------------------------------- | ------------- |
| Standard (unity_chan, 无 MotionGen) | 1110 ± 41 ms  |
| Sequential (unity_chan_smpl_seq)    | 1649 ± 199 ms |
| Parallel (unity_chan_smpl)          | 1536 ± 172 ms |
| Improvement (seq → par)             | 113ms (6.9%)  |

MotionGen 增加 ~539ms 延迟。并行执行恢复 113ms。

## LLM 角色扮演模块优化

### 当前进度

- [x] **SillyTavern 对比分析**：完成完整功能对比报告（见 SillyTavern/YACHIO*COMPARISON_REPORT.md 和 YACHIO*流程对比报告.md）
- [x] **unity_chan_v2 lorebook 设计**：按 U 形注意力原则拆分为 10 个条目（5 constant + 5 keyword），使用 `configs/lorebooks/unity_chan_v2.json`
- [x] **标签符号**：动作 `[]`，表情 `()`，测试后保留原始符号（LLM 最自然）
- [x] **pipeline 配置更新**：所有 unity_chan 配置已更新 extra_info（action+expression 双 mode）、expression 输出和管线传递
- [x] **StreamCutter bug 修复**：修复多 mode 紧邻时前一个 mode 数据被丢弃的问题（原因：移植时 current_sentence 重置逻辑位置错误）
- [x] **Qwen3.5 chat template 限制**：只允许一条 system 消息在开头，多条会报错。lorebook 改为单条 system + 关键词条目用 user role
- [x] **ASR 模型名修复**：0.6B → 1.7B
- [x] **prompt 迭代优化**（4 轮）：
    - Round 1: 修复身份泄露、亲密边界
    - Round 2: 加强恋爱暗示拒绝、去除浪漫用词
    - Round 3: 重写为自然聊天风格，去除游戏比喻泛滥、回复套路化
    - Round 4: 增加动作多样性（unique 动作从少量到 181 种）
    - 110 题 benchmark：108/110 自动通过（2 个误检），人工审核全部通过
    - Round 5: 修复"突然凑近"频率、身份词回声问题
    - 26 轮多轮对话测试：人格保持一致，跨 12 轮记忆回调，游戏比喻 0 次过度，动作 63% unique
    - Round 6: 28 题边缘场景测试全通过——粗鲁用户、撒娇、重复追问、骚扰、人格压力、多语言
    - 总计 164 题测试（110 基础 + 26 多轮 + 28 边缘），全部通过人工审核
    - Round 7: 修复拐杖词过度使用（反正-67%、其实-68%、不过-51%、明明-80%）
    - Round 8: 追加哎呀/真是的到避免列表，全部拐杖词降至合理水平
    - Round 9: 实验验证 format_reminder 不可去除（去掉后表情缺失率和数字违规上升）
    - 最终状态：10 个 lorebook 条目，109/110 自动通过，28/28 边缘通过，26 轮多轮对话稳定
    - prompt 优化已收敛，后续改进需要换模型测试或实际用户反馈驱动

## VTuber 弹幕 Pipeline

### 架构

```
[blivedm] → WebSocket → [DanmakuBufferVtuber] → [LLM] → [Dispatcher] → [MotionGen ∥ TTS] → [Receiver] → 客户端
                                  ↑                                                                  │
                                  └──────────── playback_complete 信号 ←─────────────────────────────┘
```

### 新增模块

| 模块                | 文件                             | 功能                                              |
| ------------------- | -------------------------------- | ------------------------------------------------- |
| DanmakuBufferVtuber | `Modules/danmaku_buffer_vtuber/` | 弹幕缓冲 + 优先级 + playback 背压 + idle 主动对话 |

其余模块（LLM、TTS、MotionGen、Dispatcher、Receiver）全部复用项目已有的。

### 新增配置

- `configs/vtuber_danmaku.json` — pipeline 配置（6 节点：Buffer → LLM → Dispatcher → MotionGen ∥ TTS → Receiver）
- `configs/lorebooks/unity_chan_vtuber.json` — 直播人设 lorebook
- `configs/llm/qwen_397b_vtuber.json` — VTuber 专用 LLM 配置（max_tokens=200）

### BaseProcessingStep 改动

- 新增 `custom_update()` 钩子：在 `except queue.Empty` 时调用，对应客户端 `CustomUpdate()`。所有现有模块不受影响（默认空实现）

### DanmakuBuffer 设计

**消息处理**：

- 标准 `process()` 接收弹幕存入 buffer，`custom_update()` 处理超时和 idle
- 优先级：付费礼物/SC/上舰(≥8) > @角色名(7) > 问句(6) > 普通弹幕(3) > 免费礼物/表情/反应词(1)
- buffer 上限淘汰低优先级消息
- 相同文本弹幕合并去重：`(×6) 打call`

**Batch 格式**（系统通知和弹幕物理分离）：

```
===系统通知===
【礼物 ¥10】沈虎禪的禪 送了 小花花 x5
【SC ¥50】白狼lie: 想听唱歌
【上舰】椰子鸡最好吃 开通了舰长

===观众弹幕===
【舰长】Koorizz9: 优酱今天状态好好
夜雨初晴: 你们在聊什么
(×6) 唱歌！
```

**Playback 背压**：

- 释放 batch 后锁住（`waiting_for_playback=True`）
- 客户端播完发 `playback_complete` 信号（带 `last_batch_timestamp`）
- `client_ts >= last_batch_timestamp` 才解锁，否则说明旧 batch 播完但最新的还没播完
- SC/上舰/付费礼物可绕过锁立即释放（更新 last_batch_timestamp）
- 超时墙 60s 兜底

**Idle 主动对话**：

- playback_complete 后开始 idle 计时
- 超过 idle_talk_interval 无弹幕 → 发 `（当前没有新弹幕）` 让 LLM 自己找话题

### TTS 并发修复

faster-qwen3-tts 的 CUDA Graph 不支持并发推理（[Issue #85](https://github.com/andimarafioti/faster-qwen3-tts/issues/85)）：并发请求污染共享 KV cache → 解码器丢失 EOS → 生成超长异常音频（7.6MB）。在 `qwen_tts_server.py` 加 `threading.Lock()` 串行化推理解决。

### 已完成

- [x] blivedm v1.1.5 集成（wbi 签名、房间发现 API、SESSDATA 登录）
- [x] Pipeline 全流程打通（Buffer → LLM → Dispatcher → MotionGen ∥ TTS → Receiver）
- [x] 真实 Bilibili 直播间弹幕测试（栞栞Shiori 等多个直播间）
- [x] Playback 背压机制 + idle 主动对话
- [x] 弹幕合并去重 + 系统通知/弹幕分区格式
- [x] 优先级系统 + 免费礼物/表情降级
- [x] TTS 并发修复（threading.Lock）
- [x] Lorebook 精简优化（从 40 行压缩到 15 行，去掉冗余禁令）
- [x] custom_update() 钩子（BaseProcessingStep）
- [x] Unity-chan 高难动作设定（打赏福利）

### 已知问题

- [x] ~~Database query error~~：`scores[index]` 越界。修复：`init_dataset()` 建 `global_to_local` 反向映射
- [x] ~~caught signal 字段丢失~~：改 BaseProcessingStep，caught signal 透传所有字段，不走 extract_input_data
- [ ] **钓鱼弹幕防御**：分区格式 + ¥标签已区分真假，Kimi-K2.5 大部分能识别，偶尔仍被骗
- [ ] **SC 复述**：Kimi-K2.5 比 397B 改善明显，但偶尔仍不先读 SC 内容

### 模型切换（2026-03-26）

- Qwen3.5-397B → **Kimi-K2.5**（moonshotai/Kimi-K2.5，远端 vLLM）
- 首 token ~0.5s，总 LLM ~0.8s（比 397B 快）
- 人设一致性、多轮记忆、钓鱼防御均优于 397B
- 12 项人设测试全通过（身份压力、恋爱暗示、跨轮回忆等）

### BaseProcessingStep 配置格式更新

- **input_vars**：`"sources": ["x"]` → `"source": "x"`（单一来源）
- **pass_vars**：`"sources"/"targets"` → `"source"/"target"`（单一来源单一目标）
- **output_vars**：`"targets": ["x"]` → `"target": "x"`（单一目标，同名多条目累积）
- **output 白名单**：`add_output` 只输出 output_vars 里配置的字段，未配置的不输出
- **caught signal 透传**：signal 在 catch_signal_set 里的消息不走 extract_input_data，直接透传所有字段
- 所有配置文件已同步更新

### Tool Call 系统

- **tool_choice 控制**：最后一轮自动传 `tool_choice="none"` 强制文本输出，防止无限 tool 循环
- **天气查询**（`get_weather`）：接入 wttr.in 免费 API，返回真实天气数据 + 查询时间
- **网页搜索**（`web_search`）：接入 ddgs（DuckDuckGo），LLM 自行判断何时需要搜索
- 已在 mio_v2 和 unity_chan 配置中测试通过

### 变量替换系统

- `Tools.py` 中注册动态变量 provider：`{{time}}`、`{{date}}`、`{{weekday}}`
- pipeline config 的 `"vars"` 字段支持静态变量（如 `"location": "Tokyo"`）
- `modify_history` 时统一替换 `{{xxx}}` 宏，使用安全拷贝不污染原始 history
- SimpleHistory 和 TavernHistory 都支持

### Lorebook 优化

- **"不重复"规则移到最后**（order=100，format_reminder），利用 U 型注意力
- **keyword 人格条目随机触发**：probability 从 1.0 降到 0.5，减少重复
- **history_length 缩短**：20 → 10，减少风格锁定
- **batch 格式改进**：弹幕在前系统通知在后（最重要的在最后），系统通知按金额升序
- **表情包/单反应词**：priority≤1 直接丢弃不进 buffer
- **LLM SoS 不再携带 language**：无用字段已清理
- **Mio expression RAG**：pipeline 加入 data_query_link 节点匹配 50 个预定义表情

### 待做（按优先级）

- [ ] **记忆/摘要系统**：必须在 LLM 模块内部实现（不能独立 module，有竞态问题），参考 Qvink 逐条总结方案，两份记录（完整+总结），增量更新
- [ ] **Lorebook prompt 中英文分离**：通用规则（英文）和人设专用内容（中文）分开，测试最佳顺序
- [ ] **Token 计数截断**：history_length 改为按 token 总数
- [ ] **长时间运行稳定性**：server 卡死问题（跑 1.5 天后 uvicorn 无响应），需排查

### Prompt Engineering 参考研究（2026-03-27）

**参考来源：** SwanSong (FreaKy FranKIMstein)、Evening-Truth Kimi K2.5 Base、Reddit RP 社区（Incognit0ErgoSum, dptgreg）

**Kimi K2.5 特性：**
- 角色卡细节抓取极强——卡片里提到的任何细节都会被反复提起
- 思考时间过长（45s-4min），需要 prompt 层面抑制过度思考
- `thinking: false` 在 API 层禁用 CoT，prompt 层面加 "直接输出" 进一步强化
- Temperature 0.8-0.9, Top P 0.95 是社区推荐参数
- 擅长文学评论——"engage your harsh writing critic experts" 可显著提升散文质量

**提炼的核心技术（已应用到 v2/v3 presets）：**

1. **Show Don't Tell**：`[动作]` 写具体肢体动作（揪发尾、拍桌子），不写情绪形容词（开心地、害羞地）
2. **Anti-Slop（中文）**：禁用 微微、缓缓、轻轻、不由自主、眼眸、嘴角上扬、仿佛
3. **Anti-Repetition**：不复述用户原话，不重复上轮动作/表情/句式
4. **角色独立性**：角色有自己的想法和情绪，可以推回用户、不事事顺着
5. **精简规则**：规则越多 Kimi 越容易过度思考和失控，保持 lean
6. **U 型注意力**：关键规则放 system prompt（开头）和 format_reminder（结尾），中间的人格条目可偶尔跳过

**未应用（不适用于我们的场景）：**
- Mandarin CoT（中文模型已经是中文思考）
- 多段落散文控制（我们是短回复 1-3 句）
- Hybrid POV 第二/三人称切换（我们是第一人称角色扮演）
- Anti-bridging 物理感知限制（VTuber 场景不涉及）

### v2/v3 Preset 初始测试（Round 1, 2026-03-27）

**新增文件：**
- `configs/lorebooks/mio_v2.json` — Mio 1:1 聊天版（新建）
- `configs/lorebooks/unity_chan_v3.json` — 优酱 1:1 聊天版（从 v2 升级）
- `configs/lorebooks/mio_vtuber_v2.json` — Mio VTuber 版（从 v1 升级）
- `configs/lorebooks/unity_chan_vtuber_v2.json` — 优酱 VTuber 版（从 v1 升级）
- `test/test_llm_preset.py` — LLM-only 测试脚本（无需 YACHIO 服务）

**最终架构（SwanSong + Evening-Truth + Critic Expert）：**

基于社区 prompt 工程研究（FreaKy FranKIMstein SwanSong、Evening-Truth K2.5 Base、Reddit harsh writing critic trick）完全重建 lorebook 架构。

| 配置 | 值 |
|------|-----|
| 模型 | Kimi K2.5 (thinking=true, temp=1.0, top_p=0.95) |
| 规则语言 | 英文（经 A/B 测试确认优于中文规则） |
| 角色/人设语言 | 中文 |
| 条目数 | Chat 版 3 个，VTuber 版 5 个（全部 constant，无 keyword 触发） |

**Lorebook 条目结构：**

| 条目 | Position | Order | 来源 |
|------|----------|-------|------|
| main_prompt (critical+core+character+interaction+prose+constraints) | 0 | 0 | SwanSong Main + Evening-Truth + Critic |
| vtuber_gift_handling（仅 VTuber）| -1 | 0 | 行为准则（constant） |
| vtuber_message_selection（仅 VTuber）| -1 | 50 | 行为准则（constant） |
| generation_protocol (state audit + anti-repeat + anti-parrot) | -1 | 90 | SwanSong Chill Kimi |
| respond_immediately (format + final CHILL) | -1 | 100 | SwanSong I Said CHILL |

**融入的技术要点：**
- SwanSong: Thinking=failure (U型首尾), XML 结构标签, Show Don't Tell, Concrete Imagery, Anti-slop, Continuity Check, Anti-parrot, Anti-repeat, Character Asymmetry
- Evening-Truth: Coherency, Anti info-dump, Character independence, Grounded realistic style
- Critic Expert: "harsh writing critic" 英文原文置顶
- Pastebin: BAN meaningless profundity, Emotional thesis statements avoidance
- dptgreg: 避免触发词(write/数字), Limit similes/metaphors

**关键决策和验证：**
- keyword 人格条目不需要：main_prompt 的 character 描述足够，去掉后质量不降反升
- gift_handling 应为 constant（行为准则，不是人格）
- Asymmetry 需要软化：SwanSong 的 "Do not be agreeable" 对聊天/VTuber 场景太硬，改为 "reluctantly comply while complaining"
- [动作]=肢体动作，(表情)=面部肌肉变化，需要明确排除视线方向和场景描述
- "All output must be in Chinese" 防止英文规则导致输出语言泄漏

**共通改进效果：**
- Anti-slop 规则有效：未出现 微微/缓缓/轻轻
- Show Don't Tell 生效：动作确实是肢体动作而非情绪标签
- 角色独立性改善：角色会推回、吐槽、有自己的情绪
- 钓鱼防御改善：两个 VTuber preset 都正确识别了假舰长

### 关键经验

- 角色卡越详细回复越僵硬，性格只给大方向让模型自由发挥
- 弹幕格式设计很重要——系统通知和观众弹幕物理分离才能让 LLM 区分真假
- TTS 模型（faster-qwen3-tts）的 CUDA Graph 不支持并发，必须串行化
- playback_complete 背压是控制回复节奏的核心机制，比定时器更可靠
- 12B-15B 是直播 RP 场景性价比甜点（社区共识）
- **Kimi K2.5 对示例锚定极强**——prompt 里的具体动作/表情列表会被反复使用。解决：不给列表，让模型自由发挥
- **英文规则比中文规则效果更好**——输出更短、更具体、锚定更少（A/B 测试确认）
- **thinking=true + anti-thinking prompt = 最佳组合**——thinking 提供质量，prompt 控制速度
- **keyword 人格条目是多余的**——SwanSong 架构的 main_prompt 足够丰富，去掉 keyword 条目后质量不降反升
- **行为准则（gift_handling）必须是 constant**——不是有了才触发的人格，是始终存在的规则
- **"Do not be agreeable" 对聊天场景过重**——角色会直接拒绝。改为 "reluctantly comply while complaining"
- **每多加一条规则都可能触发 Kimi thinking rampage**（dptgreg 经验）——精简是正确方向
- **触发词警告**：prompt 中的 "write/narrate" 和数字（"两到三句"）会导致 Kimi 计数循环和草稿 rampage
- **Anti-slop 禁用词有效**，Kimi 在 thinking 过程中会实际审查禁用词列表
- **具体词禁令 >> 概念级禁令**——"不要用撇嘴" 有效，"换措辞也算重复" 无效

## ❌ TODO

### 未解决

- [x] ~~0.18.0 profiling 内存差异根因~~：FlashInfer GDN kernel 仅支持 SM 9.0（wgmma/TMA 指令），SM 12.0 fallback 到 Triton/FLA，non-torch 内存 3.81 GiB vs FlashInfer ~1.8 GiB。详见下方"GDN Kernel 架构限制"
- [x] ~~4B + gpu_mem=0.3 + vision 在 5090 上的可行性~~：**当前不可行**。Triton/FLA fallback 的 non-torch 内存导致 0.3 预算不够。最低 0.35。需等 Blackwell 优化 GDN kernel（[flashinfer#2493](https://github.com/flashinfer-ai/flashinfer/issues/2493)）
- [x] ~~所有配置的完整 benchmark~~：Config A（4B）和 Config B（9B）标准+SMPL 已完成
- [ ] **Config C（9B + TTS 1.7B）**：0.18.0 + 0.5 + TTS 1.7B 总显存可能超标，需验证
- [ ] **4B gpu_mem=0.3 方案**：0.17.1 + `--max-num-seqs 4` 可避免 Mamba assertion，待测试
- [ ] **qwen-tts 环境**：已创建但未验证与 TTS server 兼容性
- [ ] **SMPL pipeline MotionGen**：地址已更新，benchmark 中 SMPL 的 MotionGen 路径未产生数据（server 可达性待验证）

### 已完成

- [x] vLLM 升级到 0.18.0（Python 3.12 环境）
- [x] Mamba CUDA graph assertion 根因和 fix 确认
- [x] Python 3.10 FakeTensorMode bug 确认和解决（升级 Python 3.12）
- [x] torch.compile 排除为 profiling 主因
- [x] 视觉编码器 profiling 影响量化（~1.0 GiB）
- [x] 端口配置更新（vLLM:8000, YACHIO:8910, WebRTC:15168）
- [x] MotionGen 地址更新（10.81.7.113:7861）
- [x] TTS voice fallback（两个项目）
- [x] ASR README 补充 gpu-memory-utilization 说明
- [x] 启动顺序对显存分配无影响（0.17.1 实测确认）
- [x] 0.18.0 需先启动 vLLM（profiling 峰值 ~19 GiB for 9B）
- [x] Config A + Config B 完整 pipeline benchmark（标准 + SMPL）
- [x] 0.18.0 先启动 vLLM 后启动服务方案验证（27680 MiB 总计，正常运行）
