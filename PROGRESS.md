# YACHIO Server Progress

## 端口配置

| 服务 | 端口 |
|------|------|
| vLLM (LLM) | 8000 |
| YACHIO 主服务器 | 8910 |
| WebRTC | 15168 |
| ASR | 8010 |
| TTS | 8011 |
| Database | 8100 |

## 环境配置

| 服务 | Conda 环境 | Python | 包版本 |
|------|-----------|--------|--------|
| vLLM | vllm | 3.12 | vllm==0.18.0, torch==2.10.0+cu128 |
| ASR | qwen-asr | 3.11 | qwen-asr (vLLM backend) |
| TTS | qwen-tts | 3.10 | faster-qwen3-tts |
| Database | database | 3.10 | sentence-transformers, faiss |
| YACHIO | yachio | 3.12 | uvicorn, fastapi |

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

| 配置 | vLLM | gpu_mem | Available KV | 推理 |
|------|------|---------|-------------|------|
| 4B + vision | 0.17.1 | 0.3 | **3.74 GiB** | Mamba assertion |
| 4B + vision | 0.18.0 | 0.3 | **0.04 GiB** | 卡死 |
| 4B + vision + mode=0（无torch.compile） | 0.18.0 | 0.3 | **0.04 GiB** | 卡死 |
| 4B + image=0（跳过图片profiling） | 0.18.0 | 0.3 | **0.46 GiB** | ✓ |
| 4B + language_model_only | 0.18.0 | 0.3 | **1.06 GiB** | ✓ |
| 4B + vision | 0.18.0 | 0.35 | **1.61 GiB** | ✓ |
| 4B + vision | 0.18.0 | 0.4 | 充足 | ✓（12960 MiB） |

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

| 模型 | gpu_mem | VRAM |
|------|---------|------|
| 4B + language_model_only | 0.3 | 10044 MiB |
| 4B + vision | 0.35 | 11250 MiB |
| 4B + vision | 0.4 | 12960 MiB |
| 9B + vision | 0.5 | 15880 MiB |

**vLLM 0.18.0 + Python 3.12，所有服务同时运行（yachio env 服务器，vLLM 先启动）：**

| Config | DB | ASR 0.6B (0.15) | TTS 0.6B | LLM | Total |
|--------|-----|----------------|---------|-----|-------|
| B: 9B (gpu_mem=0.5) | 2778 MiB | 5798 MiB | 3198 MiB | 15880 MiB | 27680 MiB |
| A: 4B (gpu_mem=0.35) | 2806 MiB | 5798 MiB | 3398 MiB | 11512 MiB | 23540 MiB |

注：9B 必须先启动 vLLM（profiling 峰值 ~19 GiB），再启动其他服务。4B 无此限制。

### 完整 Pipeline Benchmark（vLLM 0.18.0，yachio 服务器 port 8910）

测试方法：`test/test_all_configs.py`，5 轮（首轮 warmup 排除），unity_chan / unity_chan_smpl config。

**Config B: 9B + TTS 0.6B（gpu_mem=0.5，27680 MiB）**

| Pipeline | ASR | LLM total | TTS 1st | Server FA | E2E FA | E2E Total |
|----------|-----|-----------|---------|-----------|--------|-----------|
| Standard | 28±1ms | 290±88ms | 875±323ms | 1101±331ms | **1139±347ms** | 2228±754ms |
| SMPL | 28±0ms | 489±106ms | 841±181ms | 1128±191ms | **1276±65ms** | 3851±1150ms |

**Config A: 4B + TTS 0.6B（gpu_mem=0.35，23540 MiB）**

| Pipeline | E2E First Audio | E2E Total |
|----------|----------------|-----------|
| Standard | **1495±779ms** | 2952±406ms |
| SMPL | **1176±25ms** | 2470±909ms |

注：4B per-stage 日志解析间歇性失败，E2E 数据从 WebSocket 直接测量。

**与 Technical Report 原始数据对比（SenseVoice + 9B + BertVITS2）：**

| 指标 | TR 原始 | 0.18.0 9B Standard | 0.18.0 9B SMPL |
|------|--------|-------------------|----------------|
| Server first audio | **1060±22ms** | **1101±331ms** | **1128±191ms** |
| E2E first audio | 1101±32ms | 1139±347ms | 1276±65ms |

### vLLM gpu_memory_utilization 机制

基于源码（`vllm/worker/worker.py`）和实测：
- `gpu_memory_utilization` 是**总显存**的比例
- KV cache block 数量在启动时固定
- CUDA graph 在 KV cache 预算之外额外分配 1-3GB（[vllm#14632](https://github.com/vllm-project/vllm/issues/14632)）
- 启动顺序**不影响**显存分配（实测 vLLM 先启动 vs 后启动，VRAM 相同）
- 运行中杀其他 GPU 服务后 PyTorch CUDA caching allocator 会占住释放的显存

### GDN Kernel 架构限制

Qwen3.5 的 GDN (Gated Delta Net) 层有三种 kernel 实现，按 GPU 架构和 vLLM 版本分配：

| GPU | SM | vLLM 0.17.x | vLLM 0.18.0 | non-torch memory |
|-----|-----|-------------|-------------|-----------------|
| 4090 | 8.9 | 旧 C++ `gdn_attention_core` | Triton/FLA `forward_native` | 0.17: ~1.8 GiB / 0.18: 3.81 GiB |
| H200 | 9.0 | 旧 C++ | FlashInfer `forward_cuda` | ~1.8 GiB |
| 5090 | 12.0 | 旧 C++ | Triton/FLA `forward_native` | 0.17: ~1.8 GiB / 0.18: 3.81 GiB |

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

| 配置 | VRAM | Available KV | first_token | first_sentence | total (200 tokens) |
|------|------|-------------|-------------|----------------|---------------------|
| 0.17.1 无 prefix cache | 16236 MiB | 5.26 GiB | 20ms | 35ms | 1037ms |
| 0.17.1 + prefix cache | 16238 MiB | 5.26 GiB | 20ms | 35ms | 1046ms |
| 0.18.0 无 prefix cache | 15880 MiB | 1.56 GiB | 20ms | 35ms | 1027ms |
| 0.18.0 + prefix cache | 15880 MiB | 1.56 GiB | 21ms | 36ms | 1036ms |

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

| 指标 | 值 |
|------|---|
| non-KV total | 2.81 GiB（weights 1.53 + torch_peak 1.09 + non_torch 0.2） |
| 最低 gpu_mem（max_model_len=4096） | 0.12（VRAM 4340 MiB） |
| gpu_mem=0.15（max_model_len=4096） | 可行，约 4.7 GiB |

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
|-------|-------------|-------------|----------------|
| 1 | 966 | 966 | 1372 |
| 2 | 2070 | 2072 | — |
| 3 | 2878 | 2883 | 5427 |
| 5 | 12234 | 26680 | 26691 |

1-3 用户性能与旧管线（SenseVoice+BertVITS2）持平。5 用户大幅退化（vLLM 0.18.0 KV cache 1.56 GiB，5 并发超出容量）。

**WebRTC Streaming（test_webrtc.py，unity_chan_webrtc config）：**
- Duration: 45.2s
- Audio frames: 2262 sent, 2259 received
- Video frames: 1357 sent, 1357 received
- DataChannel messages: 901
- ASR: "这是一段测试音频。" ✓

**Motion Generation SMPL（5 rounds, first=warmup, MotionGen at 10.81.7.113:7861）：**

| Config | First Audio |
|--------|------------|
| Standard (unity_chan, 无 MotionGen) | 1110 ± 41 ms |
| Sequential (unity_chan_smpl_seq) | 1649 ± 199 ms |
| Parallel (unity_chan_smpl) | 1536 ± 172 ms |
| Improvement (seq → par) | 113ms (6.9%) |

MotionGen 增加 ~539ms 延迟。并行执行恢复 113ms。

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
