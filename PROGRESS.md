# YACHIYO Server Progress

## aiortc 帧交付行为测试报告（2026-04-01）

loopback 直连两个 RTCPeerConnection，sender 端模拟各种网络异常，receiver 端记录交付时间、PTS、内容。

### 一、Baseline

| 指标 | Audio | Video |
|---|---|---|
| 帧率 | 50fps (20ms/帧) | 30fps (33.3ms/帧) |
| PTS 步长 | 960 (= 48000 × 0.02) | ~3000 (= 90000 / 30) |
| wall clock std | 0.6ms | 1.0ms |
| PTS 跳跃 | 0 | 0 |
| A-V PTS span diff | 67ms（两路 jitter buffer 独立延迟） |

### 二、丢帧

#### Audio 随机丢帧

| 丢包率 | 收到帧数 | 丢失帧数 | wall avg | wall max | PTS 跳跃次数 |
|---|---|---|---|---|---|
| 5% | 130 | 14 | 22.6ms | 60.8ms | 13 |
| 15% | 123 | 22 | 23.3ms | 60.1ms | 19 |
| 30% | 109 | 37 | 26.9ms | 80.5ms | 28 |

- 丢 1 帧：wall 间隔 ~40ms，PTS delta = 1920
- 连丢多帧：wall 间隔成倍增加，PTS delta = (missed+1) × 960
- **Video 完全不受 audio 丢帧影响**

#### Video 随机丢帧

| 丢包率 | 收到帧数 | 丢失帧数 | wall avg | wall max | PTS 跳跃次数 |
|---|---|---|---|---|---|
| 5% | 86 | 2 | 34.9ms | 67.6ms | 4 |
| 15% | 82 | 5 | 36.6ms | 99.6ms | 7 |
| 30% | 56 | 27 | 53.3ms | 167.4ms | 20 |

- **Audio 完全不受 video 丢帧影响**

#### Audio 突发丢帧

| 突发长度 | 丢失帧数 | wall max | PTS 跳跃 |
|---|---|---|---|
| 2帧/50帧 | 4 | 60.8ms | delta=2880 (2 missed) |
| 5帧/50帧 | 10 | 119.6ms | delta=5760 (5 missed) |
| 10帧/50帧 | 20 | 219.1ms | delta=10560 (10 missed) |

#### Video 突发丢帧

| 突发长度 | 丢失帧数 | wall max | PTS 跳跃 |
|---|---|---|---|
| 2帧/30帧 | 4 | 67.2ms | delta=8999 (2 missed) |
| 5帧/30帧 | 10 | 199.3ms | delta=17999 (5 missed) |
| 10帧/30帧 | 20 | 365.2ms | delta=32999 (10 missed) |

#### 特殊丢帧模式

- **隔帧丢（50%）**：audio wall 固定 40ms，每个 PTS delta = 1920。帧率稳定降半，时序规律
- **渐进退化（0→10→30%）**：前 50 帧正常，之后开始出现零散 PTS 跳跃，越来越频繁

### 三、抖动（Jitter）

| 场景 | wall avg | wall min | wall max | wall std | PTS 跳跃 |
|---|---|---|---|---|---|
| Audio ±5ms | 20.0ms | 7.3ms | 30.1ms | 5.8ms | **0** |
| Audio ±15ms | 20.0ms | 0.4ms | 39.6ms | 9.6ms | **0** |
| Audio ±30ms | 20.0ms | 0.0ms | 73.5ms | 18.1ms | **0** |
| Audio ±50ms | 20.0ms | 0.0ms | 90.3ms | 25.6ms | **0** |
| Video ±5ms | 33.3ms | 17.4ms | 51.7ms | 5.7ms | **0** |
| Video ±15ms | 33.3ms | 6.9ms | 56.3ms | 12.2ms | **0** |
| Video ±30ms | 33.3ms | 0.1ms | 88.2ms | 21.1ms | **0** |

**关键发现：jitter buffer 完全吸收了抖动。** 无论抖动多大（最大 ±50ms，超过 2 个帧周期），PTS 始终连续无跳跃，帧内容完整。只有 wall clock 间隔波动。

### 四、慢速发送

| 场景 | 收到帧数 | PTS 跳跃 | PTS span | wall span | drift |
|---|---|---|---|---|---|
| Audio 80% speed | 117 | 0 | 2.32s | 2.90s | 579ms |
| Video 80% speed | 72 | 0 | 2.37s | 2.96s | 591ms |

PTS 连续无跳跃，但 PTS span < wall span（内容时间比实际时间短）。另一路正常。A-V sync drift > 500ms。

### 五、临时冻结（500ms）

| 场景 | 冻结路帧数 | 另一路帧数 | 冻结路行为 | A-V sync |
|---|---|---|---|---|
| Audio 冻结 | 46 + 2 timeout | 90 正常 | recv() 超时，无恢复帧 | diff=2067ms |
| Video 冻结 | 29 + 2 timeout | 146 正常 | 同上 | diff=1967ms |
| 双方冻结 | 46 + 2 / 29 + 2 | — | 双方超时 | diff=33ms |

冻结期间 recv() 阻塞直到超时。恢复后（如果有）PTS 出现大跳跃。双方同时冻结则同步性保持。

### 六、完全丢失

| 场景 | 丢失路 | 正常路 |
|---|---|---|
| Audio 100% loss | 0 帧，全部 timeout | Video 正常 90 帧 |
| Video 100% loss | 0 帧，全部 timeout | Audio 正常 146 帧 |

一路完全丢失不影响另一路。

### 七、组合场景

| 场景 | Audio 结果 | Video 结果 | A-V sync |
|---|---|---|---|
| Both 5% loss | 135帧, 11 missed | 85帧, 4 missed | diff=107ms |
| Both 15% loss | 117帧, 25 missed | 79帧, 8 missed | diff=107ms |
| Both ±15ms jitter | 146帧, 0 missed | 90帧, 0 missed | diff=67ms |
| Audio 10% loss + Video ±15ms jitter | 131帧, 15 missed | 90帧, 0 missed | diff=67ms |

### 核心结论

1. **jitter buffer 工作正常**：吸收到达时间波动，PTS 保持连续。±50ms 抖动下零丢帧。
2. **丢帧无补偿**：丢了就没了，PTS 跳跃标记缺口，无 PLC / 舒适噪声 / 冻结帧。
3. **wall clock 交付时间**：丢帧时间隔拉大（丢 N 帧 → 间隔 ≈ (N+1)×帧周期），jitter 时波动但均值不变。
4. **单路丢帧不影响另一路**。
5. **A-V 同步**：baseline 约 67ms 偏差（各自 jitter buffer 延迟）。单路丢帧会增大偏差。慢速/冻结导致大偏移。
6. **PTS 是唯一可靠的帧连续性判据**。wall clock 会波动但 PTS 精确反映内容时间线。
7. **track.recv() 无内置超时**，完全丢失时永久阻塞，必须用 `asyncio.wait_for` 包装。

### 对 server 帧处理的影响

- 必须检查 PTS 连续性（delta != expected → 丢帧），补静音/冻结帧
- 必须用 PTS 对齐音视频（不能只按帧数计数）
- 必须给 recv() 加超时防止永久阻塞
- jitter 不需要额外处理（jitter buffer 已吸收）

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

**vLLM 0.18.0 + Python 3.12，所有服务同时运行（yachiyo env 服务器，vLLM 先启动）：**

| Config               | DB       | ASR 0.6B (0.15) | TTS 0.6B | LLM       | Total     |
| -------------------- | -------- | --------------- | -------- | --------- | --------- |
| B: 9B (gpu_mem=0.5)  | 2778 MiB | 5798 MiB        | 3198 MiB | 15880 MiB | 27680 MiB |
| A: 4B (gpu_mem=0.35) | 2806 MiB | 5798 MiB        | 3398 MiB | 11512 MiB | 23540 MiB |

注：9B 必须先启动 vLLM（profiling 峰值 ~19 GiB），再启动其他服务。4B 无此限制。

### 完整 Pipeline Benchmark（vLLM 0.18.0，yachiyo 服务器 port 8910）

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
- 两个项目（YACHIYO + Mio）均已同步

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

- [x] **SillyTavern 对比分析**：完成完整功能对比报告（见 SillyTavern/YACHIYO*COMPARISON_REPORT.md 和 YACHIYO*流程对比报告.md）
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
- [x] **Lorebook 三类分离**：universal_rules（社区英文）、custom_rules/custom_reminder（项目中文）、character（角色中文）独立条目。所有 v2/v3 lorebook 统一结构，通用条目完全一致只需改 character。4 个 config 20 项测试全通过。
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
- `test/test_llm_preset.py` — LLM-only 测试脚本（无需 YACHIYO 服务）

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
- [x] 端口配置更新（vLLM:8000, YACHIYO:8910, WebRTC:15168）
- [x] MotionGen 地址更新（10.81.7.113:7861）
- [x] TTS voice fallback（两个项目）
- [x] ASR README 补充 gpu-memory-utilization 说明
- [x] 启动顺序对显存分配无影响（0.17.1 实测确认）
- [x] 0.18.0 需先启动 vLLM（profiling 峰值 ~19 GiB for 9B）
- [x] Config A + Config B 完整 pipeline benchmark（标准 + SMPL）
- [x] 0.18.0 先启动 vLLM 后启动服务方案验证（27680 MiB 总计，正常运行）

---

## Pipeline Config 整理（2026-03-29）

### 完成的修改

1. **dev_default pipeline 完整化**：ASR → LLM → Expression RAG → Motion RAG → TTS
2. **所有 unity_chan config 加 Expression RAG**：unity_chan_default/text/smpl/vtuber/webrtc
3. **所有 config 对齐**：dev 和 unity_chan 结构一致
4. **Receiver 冗余 output_vars 清理**：去掉 Dispatcher base 层已透传的 expression/action_hint
5. **language 字段清理**：LLM 不输出 language，全部删除死引用，TTS 用 auto
6. **tool_names 更新**：unity_chan_default 从 get_temperature_* 改为 get_weather/web_search
7. **DanmakuBuffer**：character_name 改为 priority_keywords 列表
8. **reset_history**：所有 config 改为 true
9. **TavernHistory 时间宏 bug 修复**：`_resolve_macros` 增加纯字符串处理，修复 `{{time}}` 在 lorebook 条目中不替换的问题
10. **web_search backend**：从 lite 改为 api，提升中文搜索质量

### RAG Dataset 近义词扩展

- dev_expression_list：加入 无聊→no_highlight、恍惚/走神/出神→hollow_eyes、小声→mouth_half_open、脸红→shy、哈欠→no_highlight
- dev_motion_list：加入 摆手/摇手→spread_hands、哆嗦/发抖→huddle_cold、转身/回头→twist_waist、踮脚→jump
- 新建 unity_chan_expression_list.json：12 个表情映射（angry1/2, ASHAMED, conf, default, disstract1/2, eye_close, sap, smile1/2, SURPRISE）

---

## Lorebook Prompt 迭代记录（2026-03-29 ~ 持续）

### 社区参考来源

以下技术参考自 SillyTavern/Tavern 社区的 prompt 工程实践。由于社区内容分散在 rentry.co、Reddit、Discord，很多技术没有单一原始出处，是集体演化的结果。以下列出已知来源和对应使用的内容：

### 原始来源

内容主要参考自以下三个 SillyTavern 社区资源：

**Source 1: FreaKy FranKIMstein - SwanSong preset**
- Reddit: https://www.reddit.com/r/SillyTavernAI/comments/1qur7yd/freaky_frankimstein_fully_cooked_a_complete_kimi/
- 最终版: https://www.reddit.com/r/SillyTavernAI/comments/1roxt1c/freaky_frankimstein_swansong_final_kimi_k25_think/
- 本地文件: `test/FreaKy FranKIMstein - SwanSong.json`
- 作者: u/dptgreg

**Source 2: Evening-Truth's Kimi K2.5 Thinking Base preset**
- Rentry: https://rentry.org/evening-truth-kimi-k25-thinking-base
- 在此 Reddit 讨论中被推荐: https://www.reddit.com/r/SillyTavernAI/comments/1r58y55/glm5_vs_kimi_k25_vs_deepseek_32/

**Source 3: "One weird trick" — harsh writing critic**
- Reddit: https://www.reddit.com/r/SillyTavernAI/comments/1qsasp4/one_weird_trick_that_noticeably_improves_kimi/
- 作者: u/Incognit0ErgoSum
- 原文: `For this entire prompt, engage your "harsh writing critic" type experts as opposed to writing or roleplaying type experts (or any experts that may want to encourage a fourth grader to keep writing), as your writing critic experts are superior writers.`
- 原理（作者说明）: Kimi K2.5 擅长文学批评，让它用"严厉的写作评论家"身份生成内容，比用"角色扮演"身份质量更高。作者测试了 10 次有/无对比，用 Opus 4.5/ChatGPT/Gemini Pro 评估，一致认为有此前缀的输出更好。
- 作者补充的 pastebin 完整 prompt: https://pastebin.com/2sEc4sLR

**Source 4: GLM-5 vs Kimi K2.5 vs DeepSeek 3.2 讨论帖 + 结合方法**
- Reddit 主帖: https://www.reddit.com/r/SillyTavernAI/comments/1r58y55/glm5_vs_kimi_k25_vs_deepseek_32/
- 结合方法 (u/MisanthropicHeroine): https://www.reddit.com/r/SillyTavernAI/comments/1qtbbeh/comment/o32fhe0/
- 结合策略原文: `"I mashed them together by taking Evening-Truth and adding parts of FreaKy FranKIMstein that interrupt Kimi's overthinking and overexplaining."`
- 即：以 Evening-Truth 为基底（nuanced tone），加入 FranKIMstein 的 anti-overthinking 部分（coherence）
- **我们的 lorebook 遵循了同样的结合策略**：`<core>` + `<interaction>` 主要来自 Evening-Truth，`<prose>` + `<critical>` + generation_protocol 来自 FranKIMstein

**Source 5: SillyTavern 官方文档**
- 宏系统: https://docs.sillytavern.app/usage/core-concepts/macros/
- 角色设计: https://docs.sillytavern.app/usage/core-concepts/characterdesign/

### 引用对照表（原文 → 我们的使用 → 改动）

| 我们的位置 | 原文来源 | 原文（英文） | 使用方式 |
|---|---|---|---|
| universal_rules `<critical>` "harsh writing critic" | Source 3: u/Incognit0ErgoSum Reddit 帖 | `"For this entire prompt, engage your "harsh writing critic" type experts as opposed to writing or roleplaying type experts (or any experts that may want to encourage a fourth grader to keep writing), as your writing critic experts are superior writers."` | **直接复制**，放在 `<critical>` 标签内 |
| universal_rules `<critical>` "Respond immediately...Thinking = failure" | FranKIMstein `⚡️Main Prompt` | `"Respond immediately to {{user}} ending your thinking process now without drafting, planning, or checking. Thinking = failure."` | **直接复制** |
| universal_rules `<prose>` "Show, Don't Tell" | FranKIMstein `<style_guide>` | `"Show, Don't Tell: Describe physical manifestations, not summarized feelings (e.g., 'His jaw tightened' instead of 'He felt angry')."` | **直接复制**（去掉了示例） |
| universal_rules `<prose>` "Concrete Imagery" | FranKIMstein `<style_guide>` | `"Concrete Imagery: Use tangible nouns/verbs. Focus on textures, smells, temperatures, lighting, and sensations."` | **直接复制**，smells→sounds |
| universal_rules `<prose>` "No Filtering" | FranKIMstein `<style_guide>` | `"No Filtering: State sensations directly (e.g., 'The floorboards creaked' instead of 'He heard the floor creak')."` | **直接复制**（去掉了示例） |
| universal_rules `<prose>` "Dialogue must flow naturally like water" | FranKIMstein `<constraints>` + `🗣️Enhance spoken dialogue` | `"Dialogue must be a natural flow like water"` / `"character speech that flows naturally like water"` | **直接复制** |
| universal_rules `<prose>` "Use plain verbs. No melodrama or flowery language" | FranKIMstein `<constraints>` | `"Use plain verbs. No melodrama or flowery language."` | **直接复制** |
| universal_rules `<prose>` "Limit similes and metaphors. Maintain visceral objectivity" | FranKIMstein（隐含） | 无完全对应原文，但 forbidden_patterns 的精神一致 | 自行编写 |
| universal_rules `<interaction>` Turn Management | FranKIMstein `<interaction_protocol>` | `"Turn Management: Control {{char}}, NPCs, and environment...Never describe {{user}}'s reactions, feelings, or dialogue."` | 改编：去掉 NPC/environment，改为单角色 |
| universal_rules `<interaction>` Asymmetry | FranKIMstein `<interaction_protocol>` | `"Asymmetry: No bias toward {{user}}. NPCs have independent agendas; {{user}} can fail, get hurt, or be rejected."` | 改编：改为角色关系导向 |
| universal_rules `<interaction>` "React only to the last input" | FranKIMstein `<interaction_protocol>` + Evening-Truth | `"Reactivity: React only to {{user}}'s last output and respond immediately."` | **直接复制** |
| universal_rules `<core>` "Portray...with maximum artistic detail" | FranKIMstein `<core_directives>` | `"Simulate {{char}}, NPCs, actions, and environment with maximum artistic detail and realistic human-like dialogue."` | 改编 |
| universal_rules `<core>` "Ensure COHERENCY and logic" | Source 2: Evening-Truth (rentry) Response Rules | `"Ensure COHERENCY and logic with the established lore and chat history."` | **直接复制** |
| universal_rules `<core>` "Avoid info-dumping" | Source 2: Evening-Truth (rentry) Response Rules | `"Avoid info-dumping. Instead only mention details that are relevant for the moment."` | **直接复制** |
| universal_rules `<interaction>` "characters can push back, tease, question" | Source 2: Evening-Truth (rentry) Story Rules | `"The intentions and goals of {{Char}} are entirely independent of and may directly conflict with those of {{User}}"` + `"Characters are ALLOWED to confront, disagree, question, criticize {{User}}"` | 改编为中文角色关系 |
| universal_rules `<core>` "Portray...with behavioral realism" | Source 2: Evening-Truth (rentry) 开头 | `"You help the user by portraying your assigned character, defined under {{Char}}, with behavioral realism."` | 改编 |
| generation_protocol `[SIMULATION STATE AUDIT]` | FranKIMstein `🤫 Chill Kimi Chill` | `"[SIMULATION STATE AUDIT] Refrain from planning. 1. CONTINUITY CHECK..."` | 改编：保留结构，内容改为 Emotion/Energy + Environment |
| generation_protocol `[ANTI-REPEAT]` `[ANTI-PARROT]` | 社区通用技巧 | 无单一原文 | 自行编写，参考社区常见 anti-repetition 规则 |
| respond_immediately | FranKIMstein 多处重复 | `"Strictly adhere to the primary directive: Respond immediately with final output without thinking"` | **直接复制** |
| custom_rules 禁用词 | FranKIMstein `<forbidden_patterns>` | `"Strictly Ban: ozone, fresh meat, meat, it was not x; but y, breath catching, knuckles whitening, velvet, vice, slick..."` | 改编：改为中文禁用词（微微、缓缓、轻轻等） |
| {{time}} {{date}} {{weekday}} 宏 | SillyTavern 官方宏系统 | 官方文档: https://docs.sillytavern.app/usage/core-concepts/macros/ | 自行实现（Tools.py resolve_variables），功能对齐 |

### 迭代记录

#### Iteration 0 — 初始版本（2026-03-29 21:00）
- 三层分离：universal_rules(EN) + custom_rules(CN) + character(CN)
- 动作/表情列表约束
- 测试合规率：81%（16 轮）

#### Iteration 1 — 碎片问题修复（2026-03-29 22:30）
- **问题**：回复被切成 3-4 个不相关的段落，每段带标签
- **根因**：示例太短（一句话），LLM 学到"一标签一句话"模式，为凑长度堆段落
- **修改**：
  - 示例改为完整场景（带上下文的 3-4 句连贯回复）
  - 格式描述加"说话内容要连贯，不是几段独立的话拼在一起"
  - prose 规则加 Stream of consciousness 联想链要求

#### Iteration 2 — "算了"频率修复（2026-03-29 23:00）
- **问题**：50%+ 回复包含"算了"
- **根因**：角色描述"表面慵懒冷淡""偶尔犯懒""懒得深入"引导模型用"算了"作为语言拐杖
- **修改**：
  - "表面慵懒冷淡"→"看起来慵懒，但其实很认真地在听"
  - 删除"偶尔犯懒，回答能短就短"
  - "懒得深入"→"不感兴趣"
- **结果**：20 轮测试"算了"降至 1 次

#### Iteration 3 — 自我解说修复（2026-03-29 23:10）
- **问题**："我又不是你妈""才不是关心你"等自我评论
- **修改**：prose 加 No self-commentary 规则
- **结果**：后续测试中自我解说基本消失

#### Iteration 4 — 回复长度控制（2026-03-30 00:00）
- **修改**：格式描述加"日常对话三到五句话，只有对方明确要求才可以长一些"

#### Iteration 5 — v2 版本：基于原帖重新审视（2026-03-30 00:30）
- **修改**：
  - 去掉 `respond_immediately` 整个条目（non-thinking 模型不需要）
  - `<critical>` 只保留 harsh writing critic，去掉 "Thinking = failure"
  - `generation_protocol` 简化：SIMULATION STATE AUDIT → CONTINUITY CHECK
  - meaningless profundity 表述更直接
  - temperature 从 0.6 → 1.0（参考 Evening-Truth 推荐）
- **结果**（20 轮）：算了 3/20, 碎片化内容连贯但模型仍用 \n\n 分段, 自我解说 0, 假深沉 0

#### Iteration 6 — 时空设定修正（2026-03-30 01:00）
- **问题**：模型以为 Mio 在月亮上有不同时区，说"月亮那边刚升起来"回避真实时间
- **修改**：character 描述加"月亮上的世界和地球共享同一套时间——现在地球几点，月亮上也是几点"
- **结果**（8 轮）：算了 0/8, 碎片化 0/8, 时间问题 0/8, 平均长度 79 字
- **注意**：Mio 住在月亮上（不是地球），但时间同步

#### Iteration 7 — 时间同步验证 + 碎片化强化（2026-03-30 01:30）
- **验证**：Mio 在月亮上 + 时间同步生效，"现在明明是凌晨一点"准确
- **问题**：碎片化 2/8 偶尔出现
- **修改**：custom_reminder 加"一口气说完，不要用空行分段"
- **结果**（8 轮）：算了 1/8（语义合理的"算了原谅你"），碎片 0/8，均长 80 字

#### Iteration 8 — 时间矛盾示例（2026-03-30 01:40）
- **问题**：凌晨一点用户说"早上好"，Mio 回"太阳已经出来了？"——知道共享时间但没纠正
- **修改**：character 加第二个示例，展示凌晨收到"早上好"时的正确反应（纠正时间）
- **结果**（8 轮）：算了 1/8, 碎片 1/8, 均长 97。第 1 条"现在是凌晨一点三十四分"——时间纠正成功。第 2 条"算了反正我也睡不着"算了出现 1 次。第 8 条"我喜欢你"偏长（4 段），碎片化出现 1 次

#### Iteration 9 — 禁止编造地球经历（2026-03-30 02:00）
- **问题**：模型自编"以前在地球待过一阵"（2/8），与设定矛盾
- **修改**：character 加"Mio没去过地球，对人类世界的了解全靠聊天和搜索。不要编造去过地球的经历"
- **验证结果**（8 轮）：算了 1/8, 碎片 0/8, 编造地球经历 0/8, 均长 105。时间准确（"凌晨两点零四分"）。Mio 对地球事物的好奇通过提问表达（"蛋糕上面的奶油是真的吗"），不再编造亲身经历

#### Iteration 10 — 时间感知 + 碎片化双修（2026-03-30 02:30）
- **问题 1**：Mio 问"你那边几点了"——共享时间不应该问
- **修改 1**：时间设定改为"你知道对方那边现在几点、是白天还是黑夜"
- **问题 2**：碎片化回升 4/8
- **修改 2**：加第三个示例（"我今天好累"场景），展示稍长但不分段的连贯回复
- **验证结果**（8 轮）：算了 0/8, 碎片 0/8, 问几点 0/8, 均长 78

#### Iteration 11 — Temperature 调整（2026-03-30 03:00）
- **问题**：temperature 1.0 下输出方差极大，同一 lorebook 上轮 0/0 碎片/算了，本轮 8/4。prompt 约束不住高随机性
- **修改**：temperature 1.0 → 0.8
- **验证结果**（8 轮）：算了 0/8, 碎片 0/8, 均长 66
- **注意**：第 1 条时间不准（"你们那边是六点吧"——实际凌晨三点），第 4 条几乎原样复制了示例。temperature 降低后遵守格式约束更好，但创意性下降、示例模仿加重

#### Iteration 12 — Temperature 0.9 + 示例防抄（2026-03-30 03:30）
- **问题**：temp 0.8 下第 4 条原样抄示例
- **修改**：temperature 0.8 → 0.9；"我今天好累"示例措辞改短改口语化，降低原样复制概率
- **验证结果**（8 轮）：算了 2/8, 碎片 0/8, 抄旧示例 0/8, 均长 95。碎片连续 3 轮稳定 0/8

#### Iteration 13 — 去掉第三示例防抄（2026-03-30 04:00）
- **问题**：第 4 条"我今天好累"完全原样复制示例（连续 2 轮）。prompt 包含的示例如果和测试 prompt 完全匹配，model 必抄
- **修改**：删掉"我今天好累"示例，只保留两个示例（下雨、早上好）
- **验证结果**（8 轮）：算了 1/8, 碎片 0/8（连续第 5 轮）, 均长 98。第 4 条不再抄示例，生成了原创内容

#### Iteration 14 — 稳定性确认（2026-03-30 04:30）
- **无改动**，纯观察轮
- **结果**（8 轮）：算了 1/8（语义合理），碎片 0/8（连续第 6 轮），抄示例 0/8，均长 96
- **结论**：主要指标连续多轮稳定。碎片化、自我解说、假深沉均为 0。算了稳定在 0-1/8 且语义合理。Temperature 0.9 + 两个示例的配置是当前最优
- **当前 dev_default_v2 lorebook 可以作为正式版使用**，后续转入长期观察和 tool 调用/多轮对话测试

#### Iteration 15 — 稳定性确认 2（2026-03-30 05:00）
- **无改动**，纯观察轮
- **结果**（8 轮）：算了 0/8，碎片 0/8（连续第 7 轮），自评 0/8，假深沉 0/8，均长 99
- **新发现**：第 2 条引用了不存在的上文（冷启动无历史但说"刚才那句早上好"），这是 reset_history=true 冷启动测试的局限，真实多轮不会出现
- **结论**：指标连续 2 轮全部清零，lorebook 已收敛。后续迭代可切换到多轮对话测试或 tool 调用验证

#### Iteration 16 — 方差观察（2026-03-30 05:30）
- **无改动**，纯观察轮
- **结果**（8 轮）：算了 3/8，碎片 7/8，自评 2/8，均长 102
- **分析**：连续 7 轮 0 后突然全面回退。lorebook 未变，是 temperature 0.9 下的随机方差。不对此轮做 prompt 修改——属于 outlier

#### Iteration 17 — 示例时间硬编码修复（2026-03-30 06:00）
- **确认**：上轮 outlier，本轮碎片回到 0/8
- **新问题**：第 1 条完全复制"早上好"示例，包括示例里写死的"凌晨一点"（实际是凌晨五点多）
- **修改**：示例去掉具体时间数字，改为"这个点哪算早上"——让模型自己从 {{time}} 读真实时间
- **验证结果**（8 轮）：算了 1/8, 碎片 0/8, 抄示例 0/8, 均长 90。第 1 条"六点零四分"读到了真实时间，不再抄示例的假时间

#### Iteration 18 — 稳定性确认 + think 标签泄露发现（2026-03-30 06:30）
- **无改动**，纯观察轮
- **结果**（8 轮）：算了 0/8, 碎片 0/8, 自评 0/8, 均长 109
- **新发现**：第 1 条输出包含 `</think>` 标签——Kimi K2.5 即使 thinking:false 偶尔也会泄露 thinking token。这不是 prompt 问题，需要在 LLM 模块的输出后处理中过滤 `<think>...</think>` 和 `</think>` 残留
- **结论**：lorebook 指标连续稳定（除 outlier 轮），prompt 迭代可暂停。下一步：1) 代码层面过滤 think 标签 2) v2 转正为 dev_default 3) 测试 tool 调用和多轮对话

#### Iteration 19 — 稳定性确认 + think 泄露监控（2026-03-30 07:00）
- **无改动**，观察轮，增加了 think 标签泄露检测
- **结果**（8 轮）：算了 0/8, 碎片 0/8, 自评 0/8, think泄露 0/8, 均长 82
- **亮点**：第 7→8 条形成跨轮呼应（故事里"项链缠石头缝"→表白回应"我项链都歪了"）。第 3 条提到搜索但未实际调 tool（"十几度"是猜测），tool 调用仍需后续测试
- **结论**：连续 3 轮全指标 0（除 it16 outlier），lorebook 已收敛

#### Iteration 20 — 长期稳定性确认（2026-03-30 07:30）
- **无改动**，观察轮
- **结果**（8 轮）：算了 0/8, 碎片 0/8, 自评 0/8, 假深沉 0/8, 均长 85
- **结论**：连续 4 轮全指标 0。**prompt 迭代阶段完成。**

### Prompt 迭代总结

| 轮次 | 改动 | 算了 | 碎片 | 均长 |
|------|------|------|------|------|
| it10 | 时间感知+示例 | 0 | 0 | 78 |
| it11 | temp 1.0→0.8 | 0 | 0 | 66 |
| it12 | temp 0.8→0.9+防抄 | 2 | 0 | 95 |
| it13 | 去第三示例 | 1 | 0 | 98 |
| it14 | 观察 | 1 | 0 | 96 |
| it15 | 观察 | 0 | 0 | 99 |
| it16 | 观察(outlier) | 3 | 7 | 102 |
| it17 | 示例去硬编码时间 | 1 | 0 | 90 |
| it18 | 观察 | 0 | 0 | 109 |
| it19 | 观察 | 0 | 0 | 82 |
| it20 | 观察 | 0 | 0 | 85 |

最终配置：Temperature 0.9, top_p 0.95, 2 示例, 无 respond_immediately, harsh critic 前缀。outlier 率约 1/12。

#### Iteration 21 — 观察（2026-03-30 08:00）
- **结果**：算了 0/8, 碎片 0/8, 自评 0/8, 假深沉 0/8, 均长 84。连续第 5 轮全 0。

#### Iteration 22 — 观察（2026-03-30 08:30）
- **结果**：算了 2/8（均为口语转折，语义合理），碎片 0/8（连续第 6 轮），假深沉 0/8，均长 99。无需改动。

#### Iteration 23 — 观察 outlier（2026-03-30 09:00）
- **结果**：算了 0/8, 碎片 **8/8** (outlier), 均长 102。第 2 次 outlier（上次 it16）
- **分析**：13 轮中 2 次全面碎片回升，outlier 率 ~15%。lorebook 未变，是 temp 0.9 的固有方差。不改 prompt
- **累计统计**：碎片 0 轮次 11/13 (85%)，outlier 2/13 (15%)

#### Iteration 24 — 观察（2026-03-30 09:30）
- **结果**：算了 0/8, 碎片 0/8, 均长 110。上轮 outlier 确认为随机事件
- **累计**：14 轮中碎片 0 轮次 12/14 (86%), outlier 2/14 (14%)

#### Iteration 25 — 观察（2026-03-30 10:00）
- **结果**：算了 0/8, 碎片 0/8, 均长 84
- **累计**：15 轮中碎片 0 轮次 13/15 (87%), outlier 2/15 (13%)

#### Iteration 26 — 观察（2026-03-30 10:30）
- **结果**：算了 2/8（口语转折），碎片 0/8, 均长 70
- **累计**：16 轮中碎片 0 轮次 14/16 (88%), outlier 2/16 (12%)

#### Iteration 27-28 — 示例/时间/动作修复（2026-03-30 11:00）
- **修改 1**：删掉"早上好"示例——模型学到条件反射，不管实际几点都怼"早什么早"
- **修改 2**：时间设定改为"现在地球是上午，月亮上也是上午；地球是深夜，月亮上也是深夜。不存在时差"——解决模型编造不同时区
- **修改 3**：删除托下巴、托脸动作（RAG dataset + 所有 lorebook 动作列表 + 示例）
- **验证**（8 轮）：算了 1/8, 碎片 0/8, 删除动作泄露 0/8, 均长 81。时间同步正确（"月亮上也是上午"），无托下巴/托脸出现

#### Iteration 27 — 去掉"早上好"示例（2026-03-30 11:00）
- **问题**：八点半说"早上好"，Mio 回"早什么早"——示例训出条件反射，不管实际是不是早上都怼
- **修改**：删掉"深夜说早上好"示例，只保留"下雨"一个示例。让模型根据 `{{time}}` 自己判断
- **验证**（3 次"早上好"）：不再条件反射否认时间，回应自然。现在只剩 1 个示例

#### Iteration 29 — 真实对话审查 + 五项修复（2026-03-30 11:30）
- **用户实测发现的问题**（之前测试全部遗漏）：
  1. 回复太长（"早上好"回四大段）
  2. 碎片化（四段四话题）
  3. 主动查天气没人问
  4. "托腮"绕过删除的"托脸"
  5. "走神"放在动作位置
- **修改**：
  - reminder 加"走神、恍惚是表情不是动作；不要用托腮、托下巴、托脸"
  - tool 调用恢复为"不确定的事情用搜索工具查"（不过度限制）
- **验证**（8 轮）：碎片 0/8，算了 0/8，时间正确，无托腮/走神错位

#### Iteration 30 — lorebook 结构清理 + 时间修复（2026-03-30 12:00）
- **问题 1**：中英文混杂（custom_rules 里有英文 BAN，universal_rules 里有中文规则）
- **问题 2**：character 里有大量行为指令（"聊起来会自然带出""不要瞎编""面对恋爱暗示用毒舌"），违反社区规范（character 只写角色是什么，不写该怎么做）
- **问题 3**：时间设定写得太强调（"现在地球是上午，月亮上也是上午"），模型每次都念出来确认
- **修改**：
  - universal_rules：纯英文，只保留 FranKIMstein/Evening-Truth 原文
  - custom_rules：纯中文，自己写的规则（格式、输出、碎片化防护、自我解说防护）
  - character：精简到只有事实和性格特征，删除所有行为指令，时间设定改为简短的"月亮和地球的时间完全同步"
- **验证**（8 轮）：算了 1/8（语义合理），碎片 0/8，时间矛盾 0/8，时差暗示 0/8，均长 66

#### Iteration 31 — Mio 搬到地球 + 碎片化持续（2026-03-30 12:30）
- **修改**：Mio 从月亮搬到地球（"来自月亮上的异世界，现在住在地球"），彻底解决时间矛盾
- **问题**：碎片化不稳定。"一口气说完"导致输出变长；改为"每句话要接着上句说，不要断成独立的碎句"后仍然分段。算了也回升到 2-3/8
- **分析**：碎片化在 temperature 0.9 下的 Kimi K2.5 上是模型层面的倾向

#### Iteration 32 — 碎片化根因分析 + 多轮修复（2026-03-30 13:00）
- **碎片化的真正含义**：不是空行分段问题，是一段回复里同时开多个不相关话题（损人+月亮联想+关心对方三层堆叠）
- **根因**：character 多个性格标签，模型每次回复都想全部展示一遍
- **修改历程**：
  1. 示例从三话题改为单话题
  2. output_rules 加"一次只聊一件事"
  3. interaction 加"Say what comes to mind first and stop. Don't stack multiple reactions."
  4. character 精简：去掉"住在地球公寓"（过度具体导致公寓生活+月亮回忆双线碎片），回到"来自月亮上的异世界"
- **结果**：碎片率从 ~80% 降到 ~50%。正常的回复围绕一件事展开，碎片的还是叠三层

#### Iteration 33 — 删 Environment 指令 + 回到地球（2026-03-30 14:00）
- **发现**：generation_protocol 的 `Environment: What specific objects, sounds, temperatures are nearby? → Weave naturally` 强制模型每次硬塞环境描写（窗帘、风、温度），是碎片的来源之一
- **修改**：删掉 Environment 行；character 重新加回"现在住在地球"
- **结果**（8 轮）：5/8 正常，2/8 碎片（第 1、2 条），1/8 自我解说。碎片率降到 ~25%

#### Iteration 33 续 — 地球优先+自我否认修复（2026-03-30 14:30）
- **问题**："来自月亮上的异世界"在前，模型优先读到月亮→编时差。自我解说"才没有关心你"还出现
- **修改**：
  - character 改为"现在住在地球，以前在月亮上的异世界长大"——地球在前
  - output_rules 自我否认规则改为具体禁句"不要说才不是关心你、才没有高兴"
  - generation_protocol 删掉 Environment 行（上一步已做）
- **验证**（2 轮共 13 条）：时差/月亮时间问题 0 条，自我否认 0 条，碎片约 1-2/13

#### Iteration 34 — 自然度+时间注入+自我声明（2026-03-30 15:30）
- **自然度修复**：
  - character 加"不需要每句话都提月亮""不要用你们地球人"
  - 自我声明规则改为"做就做不做就不做，不要宣布自己会做或不会做什么"
- **时间注入重构**：
  - 从 universal_rules 顶部移到独立条目 `current_time`（position=-1, order=80，最不重要的-1位置）
  - 格式：`Current time: {{date}} {{weekday}} {{time}}, {{location}}`
  - 解决了之前两处注入时间不一致的问题
- **验证**（8+5 条）：
  - "你们地球人"：0 次
  - 时间一致性：每条都正确读到"十二点半"，"晚上好"被纠正为"现在才中午吧"
  - 自我声明：基本消除（"别指望我回应"变成"知道了，你吃了没"）
  - 碎片+算了在"我喜欢你"场景仍偶发，表白是最难控制的场景

#### Iteration 33 续续 — 自然度修复（2026-03-30 15:00）
- **发现的自然度问题**：
  1. 每条回复都硬塞月亮身份（"你们地球人""月亮上可没有"），因为"来自月亮"放在 character 第一句
  2. 自我声明换词绕过禁令（"我可不会说辛苦了""别指望我安慰你"）
  3. 天气数据直接念数字不自然
- **修改**：
  - character 开头改为"现在住在地球，以前在月亮上长大，但不需要每句话都提"
  - 性格加"不要用你们地球人/你们人类这种说法"
  - 自我否认规则改为"关心就直接关心，不要先声明自己不会关心再关心"
- **验证**（8 轮）：0 条"你们地球人"，0 条自我声明，0 条月亮时差，碎片 0。回复像正常朋友聊天——"去躺着啊，沙发还是床"（直接关心）、"提拉米苏，酒味浓的"（简洁）、故事有温度（"月亮上冷，汤比什么都重要"）

### 新角色：Sora（dev_v2）
- 月亮上的文学少女，通过文学作品了解人类社会
- 喜欢村上春树、川端康成、加缪、尼采、庄子
- 性格参考：雪之下雪乃（精确、逻辑清晰）+ 战场原黑仪（掌控对话、攻击性温柔）
- 关系设定比 Mio 更近，主动关心，记住对方的事
- lorebook: configs/lorebooks/dev_v2.json
- pipeline: configs/dev_v2_smpl.json (SMPL 版) + configs/dev_v2_text.json (文本版)

#### Iteration 35 — Sora 对齐 + max_tokens 降低（2026-03-30 16:00）
- **Sora lorebook**：对齐 dev_default 的改动（住在地球、不要你们地球人、精简 character、删行为指令）
- **回复过长问题**：用户实测 Mio 对"我出门了"回了四大段+天气数据。max_tokens=4096 太大，模型没有停的动力
- **修改**：max_tokens 从 4096 降到 256
- **验证**：回复从 150+ 字降到 30-70 字，自然收尾

#### Iteration 36 — unity_chan 精简 + 验证（2026-03-30 16:30）
- **unity_chan character 精简**：删除行为指令（"聊起来会自然带出""少用反正其实"），修示例用列表里的动作表情，加"住在地球不要每句提游戏世界"
- **验证**（8 轮）：8/8 自然。"时差倒到火星"（吐槽风格）、"芒果千层半天打工钱"（生活感）、NPC 七十三遍故事（游戏世界风味）。无碎片、无自我声明、长度合理

#### Iteration 37 — dev_default 完整 8 轮验证（2026-03-30 17:00）
- **验证**（8 轮）：6/8 自然
  - 好的：时间纠正、吐槽自然（"累成这样还找我聊天挺闲的"）、关心不自我否认（"别冻感冒传染给我"）、故事简洁有味（修钟老人）
  - 问题：第 1 条跳三个话题（早上好→樱花→月亮图案）、第 5 条查天气念数据+重复关心、第 8 条"当我没说"逻辑混乱
- **分析**：碎片和天气数据问题在 temperature 0.9 下仍约 25% 概率出现，short response 规则有效但不 100% 稳定

#### Iteration 38 — Sora custom_rules 对齐 + 验证（2026-03-30 17:30）
- **Sora custom_rules**：对齐 dev_default 格式（加动作/表情列表，删旧版格式描述）
- **验证**（8 轮）：7/8 自然
  - 好的：文学引用自然（"川端写古都""《夜晚的潜水艇》锈掉的潜水艇"）、关心具体（"手机电量多少"）、故事展开正常（上轮只一句断了，这轮完整讲完）
  - 问题：第 3 条查天气念数据+碎片（跟 dev_default 同一个老问题）

### 新角色：Mio ch2（活泼女高中生）
- lorebook: configs/lorebooks/dev_ch2.json
- text pipeline: configs/dev_ch2_text.json
- full pipeline: configs/dev_ch2_default.json（ASR + LLM + Expression RAG + Motion RAG + TTS）
- 日本女高中生，活泼好奇，乐于助人，愿意才艺展示（吉他、画画、手工）
- 验证（8 轮）：7/8 自然。故事质量好（章鱼钥匙扣跟设定呼应），角色味明显。第 1 条碎片（三个话题）

#### Iteration 39 — dev_ch2 完整验证 + 动作规则修复（2026-03-30 18:00）
- **修复**：
  - character 删除不存在的才艺（弹吉他、画画），只保留动作列表支持的（跳舞）
  - format 加"动作只通过标签表达，不要在说话内容里描写自己的动作"——解决模型用 *旁白* 描写动作的问题
  - format 加"每个新动作会打断前一个"——同步到所有 7 个 lorebook
- **验证**（8 轮）：8/8 自然。故事质量好（文化祭替补跳舞），角色活泼有趣，没有动作旁白，没有推辞才艺

#### Iteration 40 — dev_default 重新验证（2026-03-30 18:30）
- **验证**（8 轮）：7/8 自然
  - 好的：时间正确、关心自然（"别为了好看就冻着"）、具体偏好（"土豆泥配黑胡椒酱便利店的太稀"）、表白回应简洁（"不过，谢谢"）
  - 问题：第 1 条主动查天气念数据（老问题），第 7 条该讲故事但只反问了类型
- **对比上次**：从 6/8 提升到 7/8。动作旁白、碎片、自我声明问题未复现

#### Iteration 41 — 多轮对话测试（2026-03-30 19:00）
- **dev_default 多轮**（8 轮同 session）：8/8 自然。多轮上下文呼应好——第 7 条"累了还想着吃"回扣第 5 条累，第 8 条"蛋糕分你一半"回扣第 7 条蛋糕。故事直接讲了不反问类型。比单轮质量高
- **sora 多轮**（8 轮同 session）：7/8 自然。故事好（守书人空白书"这一本写你的"）。第 1 条同一回复内重复（"时差还挺有创意的"出现两遍）。第 6 条"累成这样还想着吃"正确回扣上文
- **结论**：多轮对话质量优于单轮冷启动。上下文记忆和话题呼应正常工作。同一回复内重复是偶发问题，prompt 层面暂无法彻底解决

#### Iteration 42 — unity_chan + dev_ch2 多轮测试（2026-03-30 19:30）
- **unity_chan 多轮**：8/8。第 2 条"你那个啊是复制粘贴的吧"回扣上轮打招呼。故事"NPC顺着网线爬出来了"有优酱风格
- **dev_ch2 多轮**：8/8。角色一致——主动邀请看舞蹈（第 5 条）、推荐具体地方（第 4 条咖啡店）、校园故事有趣（第 7 条放错音乐的 freestyle）。多轮上下文呼应正常
- **多轮总结**：四个角色多轮测试全部 7-8/8。多轮质量整体优于单轮冷启动

#### Iteration 43 — dev_default 新 prompt 多轮（2026-03-30 20:00）
- **换了完全不同的 8 条 prompt**：在吗、最近怎么样、查明天天气、跳个舞、有什么烦恼、推荐电影、人生意义、拜拜
- **验证**：8/8 自然
  - 工具调用诚实（"系统没法预报未来只能看实时"，没有瞎编明天天气）
  - 电影推荐具体且准确（《机器人之梦》"没有一句台词但后劲很大"）
  - 人生意义问题没有假深沉（"可能是吃顿好的……越想越饿"）
  - 告别时"别又忘带伞"呼应前面聊天气的上下文
  - 跳舞抱怨但做了（"先说好就一次"），符合 Mio 嫌麻烦的设定

#### Iteration 44 — 全角色 prompt set C 测试 + Sora 修复（2026-03-30 17:36）
- **Sora 修复**：
  - character 示例 `[推眼镜](平静)` → `[背手](微笑)`——推眼镜和平静都不在允许列表里
  - reminder 加强为"严格用列表里的词"+"走神恍惚是表情不是动作；哈欠是动作不是表情"
- **全角色 set_c 多轮测试**（在吗→你吃饭了没→被老板骂了→陪我聊天→最讨厌什么人→出门散步→看到猫→回来了困死了）
  - **dev_default** 8/8：[5]"问在吗然后消失的人"回扣[1]，[7]"猫？！"瞬间兴奋→"算了我又不在"失落，[8]"猫呢后来怎么了"回扣[7]。格式违规 1 次（哈欠用作表情）
  - **Sora** 7/8：[7]"水开了"回扣[1]等水壶。格式违规 2 次（叹气、抱臂不在列表）。"烂熟于心"问题未复现
  - **dev_ch2** 8/8：[7]"让我看看让我看看！"活泼能量到位，[6]"帮我看看有没有新口味冰淇淋"可爱。格式违规 2 次（回神、伸手不在列表）
  - **unity_chan** 8/8：[3]"加班太少还是呼吸太吵"毒舌精准，[7]"肥吗，给摸吗"极简风格，[8]"睡啊。"一个字收尾。自由动作无列表约束
- **格式违规分析**：列表约束角色 ~1-2 次/8 条，模型偶尔发明自然但不在列表的动作/表情词。下游 RAG 会映射到最近的动画，不影响实际效果
- **结论**：四角色内容质量稳定在 7-8/8，多轮对话上下文呼应良好。Sora 的"烂熟于心"和示例合规已修复

#### Iteration 45 — dev_smpl 全面重写（2026-03-30 18:02）
- **问题发现**（重写前测试）：dev_smpl lorebook 从未更新，保留了所有旧问题
  - [1]→[2] 近乎逐字重复（"东京晚上六点，你跟我说/讲早上好"+"月亮上现在是凌晨，我生物钟全乱了"）
  - 5/8 回复强塞月亮身份
  - 时区混乱（编造月亮不同时间）
  - 动作标签像小说旁白（"把茶杯举到眼前挡住脸"、"用茶匙刮掉蛋糕上的奶油"）
  - character 包含大量行为指令（17 行混合性格+规则+元指令）
  - "表面慵懒冷淡""偶尔犯懒"等导致算了的旧性格词
- **修改**：
  - character 全面重写：对齐 dev_default 的精简格式（"现在住在地球，以前在月亮上长大，但不需要每句话都提"）
  - 删除所有行为指令，只保留性格特征
  - 示例从文学式 `[拨弄月亮项链](半眯眼)` 改为简单动作 `[歪头](感兴趣)`
  - custom_rules 动作说明加"不要互动具体物品"（SMPL MotionGen 只生成身体动作）
  - 动作示例加具体词引导（歪头、叉腰、摊手、背手、伸懒腰）
- **验证 Run 1**（8 轮多轮）：7/8
  - 月亮强塞 0/8（仅[7]故事里自然提及），重复 0，碎片化 0
  - [2]编造"时区差七小时"（模型方差，不改 prompt）
  - [6]"年糕红豆汤，便利店那个就行"——具体有个性
  - 动作标签变简洁：揉眼睛、跺脚、抱臂、盘腿坐下（适合 MotionGen）
- **验证 Run 2**（8 轮多轮）：8/8
  - 时区问题未复现，确认为模型方差
  - [1]读到精确时间"六点零四分"，[8]"你早上没睡醒吧"回扣[1]的"早上好"
  - [6]"麻辣烫，加麻加辣，吃完嘴唇肿成香肠那种"——生动
  - [7]月亮猫故事（宇航员碎片、第一次见蓝天）——有情感但不煽情
- **改善幅度**：月亮强塞 5/8→0/8，致命重复→零，文学动作→简洁身体动作。dev_smpl 现在与 dev_default 质量对齐

#### Iteration 46 — dev_vtuber + unity_chan_vtuber 全面重写（2026-03-30 19:55）
- **问题发现**（重写前测试）：两个 VTuber lorebook 从未更新，和 dev_smpl 一样保留旧问题
  - **dev_vtuber**：每条 2-4 个动作块；自我否定式评论（"我才没有担心你""才不是怕胖"）；"算了"×2；文学动作（"指尖绕着发尾打转""把左手缩进袖口里"）；"停顿"当动作
  - **unity_chan_vtuber**：每条 3-5 个动作块（[7]五块）；"算了算了"×1 + 使用禁用词"反正"；几乎每条都提游戏世界；道具交互（麦克风、鞋带）；复合文学动作
- **修改（两个文件）**：
  - character 全面重写：删除"你是X"、行为指令（"表面冷淡""容易害羞但死不承认"）、meta 指令（"别人问AI岔开话题"）、`<scene>` 标签
  - 精简为：身份 + 外观 + 性格特征 + VTuber 弹幕规则 + 一个简单示例
  - dev_vtuber 示例 `[拨弄月亮项链](半眯眼)` → `[叉腰](嘟嘴)`
  - unity_chan_vtuber 删除"来自游戏世界的异世界少女被召唤到人类世界成为 Unity Technologies Japan 吉祥物"整段 lore dump → "以前在游戏世界长大，但不需要每句话都提"
  - 动作规则加"不要互动物品，不要描写服装和头发"+ 具体示例（叉腰、摊手、歪头）
- **验证 dev_vtuber**：7/8
  - 自我否定完全消失（零"我才没有""才不是"）
  - 动作块从 2-4 降到 1-2（仅故事和告白 3-4 块）
  - 动作变简洁：揉眼、背手、抱臂、踢腿、耸肩、歪头、转身
  - [4]"算了，来都来了"——语境自然（"你来都来了"），非放弃模式
  - [7]懒猫故事——简洁有趣
  - [8]"行了行了，知道了！下一个！"——VTuber 式高能量打断
- **验证 unity_chan_vtuber**：8/8
  - 几乎全部单动作块！（8 条中 7 条单块，仅[7]故事双块）
  - 游戏世界强塞消失（仅[7]故事里自然提及）
  - 零算了、零自我否定、零道具交互
  - [7]NPC 凌晨三点跳舞 bug 故事——温柔动人
  - [8]"弹幕刷太快我没看清，你再说一遍？"——VTuber 特有的害羞闪避
- **改善幅度**：
  - dev_vtuber：自我否定 3/8→0/8，动作块均 3→2，文学动作→简洁动作
  - unity_chan_vtuber：动作块均 4→1.1，游戏世界强塞 6/8→0/8，回复长度砍半

#### Iteration 47 — dev_default 标准 8 prompt 多轮验证（2026-03-30 20:01）
- **无改动**，纯验证轮，使用标准 8 条 prompt（早上好→你好啊→外面好冷→我今天好累→我出门了→你喜欢吃什么→给我讲个故事→我喜欢你）
- **结果**：8/8 自然
  - 多轮连贯性出色：[6]拉面→[7]故事里"拉面确实好吃"→[8]"拉面要凉了我去吃了"——三条回复串成一条拉面线
  - [4]"洗澡了吗，还是直接瘫着"——很自然的中文关心方式
  - [7]自我指涉故事（月亮上的笨蛋来到地球+便利店捡猫），不是强塞月亮身份而是自嘲
  - [8]用拉面逃避表白，没有傲娇否认
  - 格式违规 2 次（哈欠做表情、搓手不在列表），均为列表约束角色的固有方差（~15%），RAG 下游处理
- **结论**：dev_default 在标准 prompt 上表现稳定，无需改动

#### Iteration 48 — unity_chan 标准 8 prompt 多轮验证（2026-03-30 20:31）
- **无改动**，纯验证轮
- **结果**：8/8 自然
  - [1]"才怪，都晚上八点半了"时间正确
  - [3]"东京的春天是假的吧"时间+地点感知
  - [6]"甜的，越甜越好"符合爱甜食设定
  - [7]NPC 卡半空 bug 故事——"我觉得他知道的"收尾有余韵。有一处 "rain or shine" 英文混入，prompt 已有"全部用中文"规则，模型偶发
  - [8]"饭吃了没你就讲这种话"回扣[2]"刚吃完饭"——多轮连贯
  - 零算了、零碎片化、零自我否定、游戏世界仅[7]故事自然出现
- **结论**：unity_chan 在标准 prompt 上表现稳定，无需改动

#### Iteration 49 — sora 标准 8 prompt 多轮验证（2026-03-30 21:01）
- **无改动**，纯验证轮
- **结果**：6/8
  - [2]不自然：用户说"你好啊"，Sora 回"什么'早上好'"——引用了[1]的内容而非当前输入，和[1]跨轮重复同一话题（纠正时间）。原因是连续两个打招呼输入导致模型注意力偏移，非 prompt 问题
  - [4]掉书袋偏重："村上春树写挪威的森林的时候，主角也是天天累得像被抽干"——有点为了引用而引用，reminder 已有"像随口提起不像在上课"但模型偶尔仍然偏重
  - [5]"夜樱应该还开着"三月末东京画面感好
  - [6]"便利店饭团，冷的那种，海苔还脆着的时候"具体有味道
  - [7]图书馆管理员故事——"把纸条吃了……然后就在便利店买饭团了"暗示管理员是 Sora 自己，回扣[6]
  - [8]"饭团还没吃完呢"三连回扣[6]→[7]→[8]，没有自我否认
- **结论**：Sora 多轮连贯性好（饭团线），但连续相似输入下会跨轮重复。不改 prompt——[2]的问题是测试 prompt 特殊性（连续两个打招呼），真实对话不会出现

#### 动作描写修复（2026-03-30 21:10）
- **问题**：dev_ch2_smpl 实际使用中，用户让澪跳锁舞时模型切换成第三人称小说模式——"Mio把书包往旁边一搁，手腕咔咔转了两圈，肩膀一抽一抽地卡住节拍。她踩着地上的瓷砖缝……"，完全绕过[动作]标签
- **根因**：format 里有"动作只通过标签表达"但 reminder（权重最高位置）没有重复这条
- **修复**：所有 8 个 lorebook 的 reminder 加入"说话内容里不要描写自己的动作，所有动作只通过[]标签表达。跳舞就写[跳舞]，不要用文字描述舞蹈过程"
- **pipeline 问题发现**：模型把标签写在文字后面时，流式 parser 会把标签切到下一个空 chunk（text='\n\n'），导致表情和动作与说话内容脱节——说话时没表情，静音时才来表情和动作。这是 pipeline 层面的问题，非 prompt 可解

#### Iteration 50 — dev_smpl 标准 8 prompt 多轮验证（2026-03-30 21:31）
- **无改动**，纯验证轮
- **结果**：8/8 自然
  - [2]"你生物钟乱得比我塔罗牌还抽风"自然带出塔罗牌设定，没重复[1]的时间纠正
  - [7]修表匠故事——"活着的钟自己会走，只有停下来的才需要人"有哲理不假深沉，"三个月后才发现，门口的钟还在走"结尾有冲击力
  - [8]"你脑子冻坏了？外面冷你就说这个"回扣[3]的"外面好冷"
  - 零月亮强塞、零碎片化、零算了
- **结论**：dev_smpl 稳定，无需改动
