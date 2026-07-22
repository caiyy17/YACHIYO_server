# YACHIYO Server 审计问题报告

日期：2026-07-20（Asia/Tokyo）

## 使用说明

- 本文件只记录审计发现，不代表任何项目已经修复。
- 未经最终复测的工作树修改一律不计为已修复。
- 本文件不修改产品设计，也不替代完成记录。
- 可以直接保留需要处理的编号、删除不处理的编号，再交回实施。

## 二、DataQuery、历史（init 面）

- [ ] **S11a LinkData 本地结构未校验**：dataset JSON 缺少 `data`、`keys`、`values` 等字段时没有明确的配置错误报告。
- [ ] **S12a TavernHistory lorebook 结构未校验**：必要字段（keywords、order、position、role、content）缺失或类型损坏时缺少明确的配置错误。
- [ ] **S14a playback_timeout 未严格校验**：零、负数、布尔值、NaN 或 Inf 等配置值没有统一拒绝。

## 三、失败传播（卡住类）

- [ ] **S17j OpenAI retry 未显式**：max_retries 依赖 SDK 默认，最坏等待时间不可推算。
- [ ] **S18 主服务 WebSocket send/close 无期限**：`send_text()` 无发送期限且部分异常被吞后连接状态不变，客户端半死时 send 任务可能挂起、send_queue 堆积；close 及等待 send/receive task 亦可能永久挂起，卡住 dispose。

## 五、流式并发节点

- [ ] **P05 JointStream cancel 后后台继续消耗**：pump thread 是 daemon，cancel 后仍继续拉取外部流直到自然结束，占用外部 TTS/motion 服务并发容量，拖累后续回合。

## 六、WebRTC（卡住类）

- [ ] **W06 派生任务未全部跟踪**：relay、track、pipeline、callback 等任务异常后可能失去引用，留下无人清理的半死会话。
- [ ] **W18 pipeline WebSocket open/send/close timeout 不完整**：网关到主服务的连接和发送没有完整 deadline，挂起会卡住会话建立。
- [ ] **W19 RTC 协商无期限**：setRemoteDescription、createAnswer、setLocalDescription 等步骤缺少统一期限，挂起时 offer 请求悬死。
- [ ] **W20 lane 启动无期限**：连接建立后等待 WebSocket ready、首帧和 startup accumulation 可能永久等待。
- [ ] **W21 track.recv 无期限**：音视频轨道读取没有 idle deadline，轨道断供时会话半死。
- [ ] **W22 relay 异常不上升**：relay 任务异常可能只写日志，不使 session 失败也不触发清理，留下卡死会话。

## 八、Standalone 服务

- [ ] **X11 HYMotion prompt rewrite timeout 不完整**：远程 rewrite 无可靠 deadline，挂起时占住 worker。
- [ ] **X13 HYMotion async endpoint 被同步推理阻塞**：部分 async handler 直接执行同步推理，阻塞事件循环拖垮整个服务。
- [ ] **X14 HYMotion pipeline 获取永久等待**：内部 pipeline acquire 没有期限。
- [ ] **X16 HYMotion 客户端断开不取消 worker**：请求已失效后推理仍继续占用 GPU，拖累后续请求。
- [ ] **X25 VADServer 孤儿 session**：调用端放弃后，服务端初始化线程仍可能继续并留下 session。
- [ ] **X26 本地推理无法被线程 timeout 终止**：线程外等待 timeout 不会停止 CUDA/模型计算；硬期限需要可终止的 worker 子进程，属于风险改动。

## 十一、需要产品决定，不应直接修改

- [ ] **D01 动作/表情映射策略**：扩充别名、调整阈值或增加 fallback 都会改变角色表现。
- [ ] **D02 qwen-asr 依赖版本**：当前 transformers 与 qwen-tts 要求不一致；环境调整需要模型回归。
- [ ] **D05 Queue 背压协议**：阻塞、拒绝、丢最新、丢最旧或独立控制通道需要明确选择。
