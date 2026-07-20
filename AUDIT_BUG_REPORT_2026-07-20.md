# YACHIYO Server 审计问题报告

日期：2026-07-20（Asia/Tokyo）

## 使用说明

- 本文件只记录审计发现，不代表任何项目已经修复。
- 未经最终复测的工作树修改一律不计为已修复。
- 本文件不修改产品设计，也不替代完成记录。
- 可以直接保留需要处理的编号、删除不处理的编号，再交回实施。

## 一、Pipeline、配置和节点

- [ ] **S09 节点异常仅记录**：`BaseProcessingStep.run()`、`SpanProcessingStep.run()` 等路径会把内部异常只写日志后继续；部分自定义 `run()` 则会静默结束线程。
- [ ] **S09a custom_update 异常静默死线程**：`custom_update()` 位于 `queue.Empty` 异常分支内，它再次抛错时不会被同级异常分支接住，也没有通知 pipeline。
- [ ] **S09b cancel/dispose 异常未上报**：`check_cancel()`、`custom_cancel()`、`dispose()` 部分调用位于节点异常边界之外，失败可能只终止单个线程。
- [ ] **S09c pipeline 无失败状态**：节点线程失败后，`ClientConnection` 仍可能保持 initialized，其他节点继续等待或处理。

## 二、DataQuery、历史和运行时 fallback

- [ ] **S11 LinkData dataset 缺失处理**：指定 dataset 缺失时原实现会回落 `default.json`，掩盖配置错误。
- [ ] **S11a LinkData 本地结构未校验**：dataset JSON 缺少 `data`、`keys`、`values` 等字段时没有明确的配置错误报告。
- [ ] **S11b LinkData load 失败继续**：`/load_dataset` 异常会返回字符串 `"error"`，返回值又被忽略，节点仍可能初始化成功。
- [ ] **S11c LinkData query 假成功**：查询失败会返回字符串 `"error"`，作为正常业务输出继续流动。
- [ ] **S11d LinkData 响应未检查**：没有完整检查 HTTP 状态、JSON 类型、`results` 结构和索引范围。
- [ ] **S12 TavernHistory lorebook 缺失被跳过**：配置的 lorebook 不存在时原实现直接跳过，初始化仍成功。
- [ ] **S12a TavernHistory 本地结构未校验**：lorebook 必要字段、strategy、logic 和索引结构损坏时缺少明确配置错误。
- [ ] **S12b Tavern 数据库加载失败继续**：Database `init_dataset()` 异常返回 `"error"`，构造过程仍继续。
- [ ] **S12c Tavern 查询错误等同无匹配**：Database `query()` 异常返回 `[]`，无法区分依赖错误和合法没有激活项。
- [ ] **S12d 空数据库仍发远程请求**：只有 constant lore 时仍可能初始化或查询不需要的关键词/向量数据库。
- [ ] **S13 history_mode 回退**：未知、错误类型或大小写错误的 `history_mode` 会回落 `SimpleHistory`。
- [ ] **S14 Danmaku playback timeout 假成功**：等待 `playback_complete` 超时后强制解锁并继续释放下一批，相当于把确认丢失当作播放成功。
- [ ] **S14a playback_timeout 未严格校验**：零、负数、布尔值、NaN 或 Inf 等配置没有统一拒绝。
- [ ] **S15 ServerVAD feed 错误被吞**：feed 失败只记日志并 return，节点继续使用已经不可靠的检测状态。
- [ ] **S15a ServerVAD reset 错误被吞**：cancel/reset 失败只记录，不使节点或会话失败。
- [ ] **S15b ServerVAD 错误恢复日志**：reset 失败后仍可能记录 detection resumed。
- [ ] **S15c ServerVAD 响应结构回退**：feed 响应缺少 `events` 时使用默认空列表，把坏响应当成没有事件。
- [ ] **S15d ServerVAD close 错误被吞**：删除 session 或关闭失败存在静默路径。
- [ ] **S15e ServerVAD stale session**：删除旧 session 或创建新 session 失败后，可能继续保留失效 session ID。

## 三、外部服务 timeout 与失败传播

- [ ] **S16 DataQuery 无默认 timeout**：LinkData 和 TavernHistory 的 load/query HTTP 请求可能永久等待。
- [ ] **S16a DataQuery 非 2xx 未失败**：缺少统一 `raise_for_status()`，服务返回 4xx/5xx 时仍可能继续解析或记录成功。
- [ ] **S17 LLM timeout 不明确**：OpenAI client、warmup 和流式 chat 没有项目明确配置的默认 timeout 和 retry 行为。
- [ ] **S17a LLM 流错误变正常结束**：`BaseLLMCaller.call_stream()` 捕获异常后结束 generator，LLM 节点仍可能发送成功 EoS。
- [ ] **S17b ASR 错误变正常结果**：ASR 请求失败会返回类似 `("error", "auto")` 的值，继续作为识别结果处理。
- [ ] **S17c TTS 错误变空音频**：TTS 请求失败会返回空 bytes 或正常结束流，后续可能输出零长度音频或成功 EoS。
- [ ] **S17d Motion 普通请求假成功**：Motion 请求失败返回空字符串，调用方无法区分失败和空结果。
- [ ] **S17e Motion SSE 结束检查不足**：SSE error、异常 EOF 或缺少 done 可能仍被当成成功，并保存旧状态或零值 continuation。
- [ ] **S17f Motion 流无总耗时检查**：当前 requests timeout 主要限制连接/读取等待，不保证整个持续流在总期限内完成。
- [ ] **S17g Tool 调用错误业务化**：Weather/Search 外部失败会被包装成普通 tool result，LLM 继续生成。
- [ ] **S17h Tool HTTP 状态未检查**：Weather 请求有固定 timeout，但缺少完整 HTTP 状态与响应 schema 检查。
- [ ] **S17i Search timeout 不明确**：DDGS 搜索没有项目明确的默认 timeout。
- [ ] **S17j retry 行为不明确**：OpenAI SDK 未锁定版本且没有显式 max retries，实际等待时间依赖 SDK 默认值。
- [ ] **S18 WebSocket send 无期限**：主服务 `send_text()` 没有明确发送期限，异常还可能被吞后继续连接。
- [ ] **S18a WebSocket receive 无期限**：`receive_text()` 没有会话级 idle/断连期限。
- [ ] **S18b WebSocket close 无期限**：关闭 socket 及等待 send/receive task 可能永久挂起。
- [ ] **S18c WebSocket 失败仍继续**：`TimeoutError`、`WebSocketDisconnect` 等发送错误存在 pass 路径，没有可靠改变连接状态。
- [ ] **S19 线程关闭 timeout 只记日志**：`wait_for_threads()` 超时后只记录仍存活线程，随后可能继续清空状态并报告 disposed。
- [ ] **S19a EventHandler join 不验证**：`EventHandler.join()` 等待后不返回或检查线程是否真正结束。
- [ ] **S19b Queue put 可能永久阻塞**：入口、节点、EventHandler 的 `Queue.put()` 在有限队列满时没有失败检查。
- [ ] **S19c custom_init timeout 无法终止线程**：初始化超时只放弃等待，daemon 初始化线程仍可继续访问依赖或修改状态。

## 四、并发、生命周期与清理

- [ ] **S21 client 生命周期竞态**：register、init、unregister 和 WebSocket 替换并发时，旧连接可能影响同 ID 的新连接。
- [ ] **S23 初始化阻塞事件循环**：async API 中同步执行节点初始化，单节点最长等待会阻塞其他请求。
- [ ] **S27 logger 创建失败不回滚**：logger 初始化失败时，client 注册集合可能留下半注册记录。
- [ ] **S28 logger 重复初始化问题**：重复 import/init 时 handler 生命周期和重复写入需要验证。
- [ ] **S29 重注册覆盖日志**：同 client ID 创建 logger 时会删除现有日志，可能覆盖仍在保留期内的记录。
- [ ] **S30 日志 API 错误混淆**：不存在、非法路径、读取失败和真正空文件没有不同响应。
- [ ] **S31 大日志全文读取**：日志 API 读取全文可能造成明显内存和响应体占用；是否限制尾部大小需要决定。
- [ ] **S33 EventHandler 内部错误被丢弃**：未知或损坏控制消息只记日志/drop，没有使内部不变量失败。

## 五、流式并发节点

- [ ] **P01 JointStream 子线程异常被隐藏**：pump thread 的 generator 异常最终只写入 `_DONE`，主节点认为该流正常结束。
- [ ] **P03 JointStream 非字典 chunk 被丢弃**：caller 返回错误结构时只记录并跳过。
- [ ] **P04 JointStream 输出字段缺失被跳过**：配置要求的 caller 输出字段不存在时没有明确结构错误。
- [ ] **P05 JointStream cancel 后后台继续消耗**：pump thread 是 daemon，cancel 后可能继续读取并丢弃外部流直到结束。

## 六、WebRTC

- [ ] **W01 config fetch 失败后回退**：WebRTC 获取主服务 config 虽有固定 timeout，但不检查非 2xx；失败后会用默认参数继续 offer。
- [ ] **W02 lane rate 契约不明确**：audio/video/data rate 是必填还是允许默认没有明确约定。
- [ ] **W03 同 ID offer 竞态**：同 client ID 的并发 offer 没有完整串行化。
- [ ] **W04 健康会话过早替换**：新 offer 在 schema、兼容性和协商完成前可能先删除健康旧会话。
- [ ] **W05 offer 与 cleanup 错误未聚合**：协商失败且 cleanup 也失败时，没有返回完整失败信息。
- [ ] **W06 派生任务未全部跟踪**：relay、track、pipeline、callback 等任务可能异常后失去引用。
- [ ] **W07 cleanup 异常被吞**：释放多个关键资源时部分异常被 pass，最终仍可能报告成功。
- [ ] **W08 registry 身份竞态**：旧 session cleanup 可能删除同 ID 的新 session。
- [ ] **W09 callback/close 错误静默**：session-end callback、pipeline WebSocket close、peer close 和客户端通知存在吞错路径。
- [ ] **W10 track 错误当正常结束**：解码、网络或 track 异常可能被当作正常媒体结束。
- [ ] **W11 DataChannel JSON 类型不足**：数组、字符串和其他 JSON 标量缺少严格 object 拒绝。
- [ ] **W13 视频尺寸和像素无明确上限**：实际入向帧可能造成过大内存或编码开销；阈值需要产品决定。
- [ ] **W14 无全局会话配额**：总 session 数和单 client session 数没有资源上限。
- [ ] **W15 图片解码失败回退**：部分解码失败路径可能使用 idle frame 继续，而不是使会话失败。
- [ ] **W16 客户端通知失败不改变状态**：通知失败仅记录或忽略，会话仍可能表现正常。
- [ ] **W17 纯音频 WebRTC 不支持**：当前 assembler 依赖音频和视频两条 lane 均启动。
- [ ] **W18 pipeline WebSocket open/send/close timeout 不完整**：连接和发送没有完整 deadline，recv 只有固定等待值。
- [ ] **W19 RTC 协商无期限**：setRemoteDescription、createAnswer、setLocalDescription 等步骤缺少统一期限。
- [ ] **W20 lane 启动无期限**：等待 WebSocket ready、音视频首帧和 startup accumulation 可能永久等待。
- [ ] **W21 track.recv 无期限**：音视频轨道读取没有 idle deadline。
- [ ] **W22 relay 异常不上升**：relay 任务异常可能只写 info，不使 session 失败。

## 七、WebUI 与浏览器客户端

- [ ] **U01 config/lorebook 路径边界不足**：读取和保存路径可能越过允许目录或经过目录外符号链接。
- [ ] **U02 tmp 权限判断不足**：只按用户提供路径判断 tmp，符号链接可能指向正式配置。
- [ ] **U03 保存非完整原子操作**：失败时可能留下截断或半写 JSON。
- [ ] **U04 URL 路径段未统一编码**：client/config/log 名直接拼入 URL 时可能改变路径含义。
- [ ] **U05 DOM 字符串拼接**：不受控 client ID 可能被浏览器解释为 HTML 或 inline JavaScript。
- [ ] **U06 同步 requests 阻塞 async route**：WebUI async endpoint 内直接运行同步 HTTP 请求。
- [ ] **U07 上游请求无 timeout**：六处 Python requests 没有明确连接/读取期限。
- [ ] **U07a 上游错误返回 HTTP 200**：timeout、连接失败和非 2xx 常被包装成 `success=false`，但 HTTP 状态仍为 200。
- [ ] **U08 配置目录缺失假空列表**：内部配置目录不存在时可能返回成功的空集合。
- [ ] **U10 浏览器 fetch 永久等待**：页面 fetch 没有 AbortController 或统一 deadline。
- [ ] **U11 日志轮询请求堆积**：旧日志请求挂起时，`setInterval` 仍会继续发起新请求。
- [ ] **U12 浏览器未统一检查 response.ok**：非 2xx 可能继续按正常 JSON 解析。
- [ ] **U13 WebRTC config 失败回退 FPS**：配置获取失败后浏览器使用默认 FPS 继续。
- [ ] **U14 disconnect/unregister 错误被吞**：断开阶段失败没有明确呈现给用户。

## 八、Standalone 服务

- [ ] **X01 QwenASR `/models` timeout 隐式**：部分 backend 请求依赖 httpx 默认 timeout，项目没有明确配置。
- [ ] **X02 QwenASR 启动永久等待**：等待 backend ready 的 while loop 没有总期限。
- [ ] **X03 QwenASR 启动错误被忽略**：启动探测异常使用宽泛捕获继续等待，缺少最终失败原因。
- [ ] **X04 QwenASR shutdown 不验证**：只 kill backend，不 wait 或检查是否真正退出。
- [ ] **X05 QwenASR health 假健康**：health endpoint 不验证 backend 是否仍存活。
- [ ] **X06 QwenTTS lock 无限等待**：模型相关 `threading.Lock.acquire()` 没有期限。
- [ ] **X07 QwenTTS 生成无硬期限**：普通和流式模型生成可能永久占用 worker。
- [ ] **X08 QwenTTS 流错误变 EOF**：响应头发送后 generator 异常，没有明确 SSE error/done，客户端可能把 EOF 当成功。
- [ ] **X09 QwenTTS 未知 voice 回退**：未知 voice 会回落第一个 voice，掩盖客户端配置错误。
- [ ] **X10 QwenTTS 模型加载无期限**：`from_pretrained()` 可能联网下载，startup 和 warmup 无总 deadline。
- [ ] **X11 HYMotion prompt rewrite timeout 不完整**：远程 prompt rewrite 没有可靠 wrapper deadline。
- [ ] **X12 HYMotion prompt rewrite 失败回退**：失败后使用原 prompt 或短时返回，掩盖外部依赖错误。
- [ ] **X13 HYMotion async endpoint 被同步推理阻塞**：部分 async handler 直接执行同步推理。
- [ ] **X14 HYMotion pipeline 获取永久等待**：内部 pipeline acquire 没有期限。
- [ ] **X15 HYMotion Queue/task 永久等待**：`queue.get()` 和 worker task 等待缺少 deadline。
- [ ] **X16 HYMotion 客户端断开不取消 worker**：请求已失效后推理仍可能继续占用 GPU。
- [ ] **X17 HYMotion ffmpeg 无 timeout**：`subprocess.run()` 可能永久等待。
- [ ] **X18 HYMotion ffmpeg 失败假成功**：下层返回 false 后，上层仍可能返回输出路径。
- [ ] **X19 HYMotion checkpoint 缺失回退**：checkpoint 不存在时可能使用随机权重继续运行。
- [ ] **X20 VectorDatabase 模型下载无期限**：SentenceTransformer 首次加载可能联网下载且无 startup deadline。
- [ ] **X21 VectorDatabase encode/query 无期限**：同步模型计算可永久占用 Flask worker。
- [ ] **X22 VectorDatabase 未知类型回退**：未知数据库 type 会回落 Simple 实现。
- [ ] **X23 VADServer lock 无限等待**：普通 lock acquire 没有期限。
- [ ] **X24 VADServer detector 初始化无期限**：Silero 模型加载/下载发生在 create 请求中且没有 deadline。
- [ ] **X25 VADServer 孤儿 session**：主服务超时后，服务端初始化线程仍可能继续并留下 session。
- [ ] **X26 本地推理无法被线程 timeout 终止**：只在线程外等待 timeout 不会停止 CUDA/模型计算；硬期限需要可终止 worker 子进程，属于风险改动。

## 九、启动、部署和长期运行

- [ ] **O01 进程识别不精确**：screen/process 检查使用模糊匹配，可能识别到其他进程。
- [ ] **O02 readiness 检查不足**：部分启动检查只看端口，不验证端点状态、响应体和依赖真实性。
- [ ] **O04 部署非事务性**：多服务启动中途失败时可能留下半部署状态。
- [ ] **O05 自动日志清理未决定**：当前只完成一次人工清理；是否加入后台 48 小时清理需要决定。
- [ ] **O06 多进程活动日志保护**：若增加自动清理，需要避免删除其他进程仍打开的日志。
- [ ] **O07 日志轮转策略未决定**：轮转周期和保留数量没有产品决定。
- [ ] **O08 公网边界风险**：公开监听、全 CORS、无认证、无限连接与无资源配额仍存在。
- [ ] **O10 长期 soak 未完成**：尚未完成至少 1.5 天的持续运行验证。

## 十、测试脚本和测试证据

- [ ] **T01 settings 失败后少测仍通过**：配置读取失败可能退化为少检查几个服务，而不是测试失败。
- [ ] **T02 health 判定过宽**：部分 HTTP 4xx/5xx 被算作服务可达或健康。
- [ ] **T03 live/server 零消息通过**：没有收到真实消息时部分测试仍可成功退出。
- [ ] **T04 WebRTC 测试隔离不足**：client/config 名称、初始化和 cleanup 没有始终严格隔离验证。
- [ ] **T05 receiver 异常被忽略**：JSON、音频解码、track 接收等错误可能不计入失败。
- [ ] **T06 FrameSplitter teardown 未逐项确认**：没有确认每个 worker 都已退出。
- [ ] **T08 backpressure 基线不足**：未形成预期压力或未处理真实数据时仍可能通过。
- [ ] **T09 辅助线程退出未检查**：join 后没有统一检查 `is_alive()`。
- [ ] **T10 WebUI 启动测试诊断不足**：startup GET 缺少可靠测试 timeout 和受限 stdout/stderr 记录。
- [ ] **T11 ffmpeg 结果未严格断言**：退出码和输出文件不总是测试交付条件。
- [ ] **T12 WebUI 负例断言过宽**：任意 `success=false` 都可能算通过，不检查错误原因和状态码。
- [ ] **T13 WAV 部分损坏可能通过**：只要已有少量好音频，后续解码错误可能未使测试失败。
- [ ] **T14 单元测试污染生产日志**：测试 import/init 可能清理或写入真实 `logs/`。
- [ ] **T15 历史 fixture 原样性**：历史输入必须保持 Git blob 字节一致，避免测试内容被审计改写。
- [ ] **T16 LLM fixture 口径有限**：164/164 只代表请求成功且输出非空，不代表语义、动作或角色行为正确。
- [ ] **T17 测试证据可能全部被忽略**：结果文件受 `.gitignore` 影响，最终证据是否提交尚未决定。
- [ ] **T19 测试死代码和病句**：存在过期常量、失效标签及批量替换产生的错误文案。
- [ ] **T20 退出码不统一**：子项、cleanup 或 timeout 失败后，测试进程不总是非零退出。

## 十一、需要产品决定，不应直接修改

- [ ] **D01 动作/表情映射策略**：扩充别名、调整阈值或增加 fallback 都会改变角色表现。
- [ ] **D02 qwen-asr 依赖版本**：当前 transformers 与 qwen-tts 要求不一致；环境调整需要模型回归。
- [ ] **D03 节点故障粒度**：一个节点异常时整条 pipeline 失败，还是隔离节点继续，需要明确决定。
- [ ] **D04 Danmaku 恢复协议**：播放确认丢失后，是关闭 pipeline、取消当前回合还是重新同步。
- [ ] **D05 Queue 背压协议**：阻塞、拒绝、丢最新、丢最旧或独立控制通道需要明确选择。
- [ ] **D08 timeout 数值与配置位置**：每项依赖的默认期限需结合真实延迟确定，不能用一个全局 timeout 覆盖全部服务。
