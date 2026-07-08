# YACHIYO Server Progress

## 近期改动

- **caller 流产出统一为 dict{产出名: 值},取消"单产出裸块/多产出 dict"的伪分叉**(用户定调:"返回一条还是多条对于代码有任何一点点区别吗?"——没有;产出个数 = dict key 数 = output 条目数,零分支):每个 caller 的 `call_stream` 一律 `yield` 一个以产出名为 key 的 dict(`OpenaiTTSCaller`→`{"audio":...}`、`MotionGenerationCaller`→`{"motion":...}`、含空音频/skeleton passthrough/尾块全部包起);消费侧同构——单流 Step 各取自己那个产出(`TTSStep` `chunk.get("audio")`、`MotionStep` `chunk.get("motion")`),`JointStreamStep._pack_chunk` 只有一个循环 `for e in outputs: pack[e["target"]] = chunk[e["source"]]`(bytes 顺手 b64、非 dict 块报错丢弃),删掉原 `len(outputs)==1` 分支。**一路吐三 key dict(audio+motion+expression)与三路各吐一 key dict 走完全相同代码、输出 pack 形态一致**——mock 双例证实(pack 字段集 = 产出集,PASS)。
- **joint streams 的 input/output 为 source/target 改名列表**(用户定调:不同流的内部名可能相同,须在 streams 配置层改名;格式与 vars 声明同构):input 的 target = caller 参数名,调用为 `call_stream(**{target: value})`(多输入 caller 如 TTS 的 prompt/language/speaker 天然支持);output 按 source→target 从 dict 块取产出;required inputs 按 source 动态推导,validate_config 查显式条目与 output target 声明。dev_joint_stream 迁移。另修实跑暴露的空白句问题:LLM 偶发纯换行句经严格 =="" 判空放行致 QwenTTS 400(机制自愈但 log 报错),判空改为空白即空(与单流 TTSStep strip 一致)。验证:mock(kwargs 映射/dict 拆包/丢未声明键)、validator、**真实 dev_joint_stream 全链实跑 PASS**(音频→ASR→LLM→link→joint,13 句;196 包中 119 包同带 audio_data+action、26 audio 尾包、51 motion 尾包,轮级 SoS…EoS + 句级 js_SoS/js_EoS 完整)。另:streams 子 config 分层与普通节点一致——model 键是 settings 条目引用名,行为参数(voice/duration/流粒度)历来在节点 config,服务定义仍全在 settings。
- **joint 流延迟分解与本地 motion 对比实测**:单流基线——TTS(QwenTTS 远程 API)首块 0.24s、稳态 4.2× 实时(每 ~225ms flush ~900ms 音频);motion 远程 18084 首块 ~1.0s(网络 0.17s + flood warmup + 客户端攒 2 个 delta,prompt 改写不在关键路径——开关实测无差)、稳态 1.5× 实时。**联合首包 = max(两侧首块)**:远程 motion 1.05s;**本地 18085(5090,stream_mei ldf)motion 首块 0.11~0.17s、稳态 ~40ms/块(6.7× 实时)、整句 150 帧 0.8s,联合首包降至 0.61s——瓶颈翻转到 TTS 侧**(QwenTTS 首块 0.24~0.6s 波动)。dev_joint_stream.json 建立并全链实跑(ASR→LLM→link→joint 四节点,10 句 30s,log 零错误):轮级 SoS(prompt)/句级 js_SoS(pass_data,keys 逐句如实变化)/配对包/audio 尾块渐短/motion 单侧尾包/纯 audio 句(LLM 无动作标记,motion 流不启动、免启动费)/纯 motion 句(text 空)——全部机制自然覆盖。已知观察:motion duration 5s 固定 vs 句音频 3~4s,句尾有 1~2s 单侧 motion(不丢不填定调的自然结果,后续可选 duration 联动)。
- **JointStreamStep 完成(通用 N 路流合并 + caller 注册表)**:`Modules/parallel/JointStreamStep.py`(注册 `call_joint_stream`)。**通用化**(用户指正:不硬编码路数、不硬编码 TTS/motion,唯一假设是各流 chunk 等时长故可打包):config 的 `streams` 列表声明 N 路,每路 `{caller: 注册名, input: 输入字段, output: 输出名, config: 子配置}`;required_inputs/required_catch_signals(任一路 continuous→SoS)均按 streams 动态推导。**caller 注册表**(用户指正:module 之间不得横向 import):各 module 在自己 `__init__` 里注册 `caller_map`(openai_tts/motion_generation),顶层聚合 `get_caller_class_by_name`,joint 按 config 名解析——与 Step 的 function_map 装配同构,换 caller 只改 config;joint 内注册表 import 延迟到 custom_init(此前顶层 import 具体 caller 曾把 pydub 依赖链拽进 parallel 包,base 环境下连 dispatcher 注册都被拖垮,已修并验证)。打包规则:每包从每条存活流各取一块(阻塞等齐,慢侧定节奏),流尽即从包中消失、其余流继续单独成包直至全尽;句级 SoS(pass_data 包)开、EoS 收;cancel 不闭包络;bytes 块自动 b64。validate_config:streams 非空、每路四键齐全、caller 已注册、output 有 output_vars 条目。验证:mock 三路不等长(4/2/3)收缩打包序精确、逐流顺序、空输入侧不启动、validate 四负例;**真实服务经注册表解析 PASS**(js_SoS→AM×10→M×7→js_EoS,首包 audio 300ms+motion 9 帧,motion 帧序 [9×16,6])。**逐包延迟实测**(长句,音频 5.04s/motion 5.0s):首包 1.05s(=max 两侧首块,motion 不增加启动成本);16 个满包全部精确 300ms+9 帧成对,尾包 240ms/6 帧;包间隔 170~260ms < 300ms 播放速度,从首包起播 play-margin 单调增至 +1.74s,**全程零欠载**——客户端无需预缓冲。尚无 config 引用。
- **motion caller 客户端精确重切(与 TTS _rechunk 对称)**:`stream_frames` 配置(默认 0=关,零影响)——服务端 delta 任意粒度(commit 取整/首块+1)在 SMPL-H 帧级缓冲,精确切出 stream_frames 帧等长块、尾块可短;增量 humanoid 转换(prev_trans/ref_y)以输出块为边界,error 中断丢弃残尾。真实服务(18084,seed 确定)实测:9 帧块序列 [9×16,6] vs 默认 [5,4×36,1],总帧数相等,拼接 root_xz/hips/joints **逐位一致**(max diff=0)。joint 对齐窗约束由此只剩 TTS 的 100ms 倍数,window_ms=300(TTS stream_chunk_ms:300 + motion stream_frames:9)即可,不受 ldf commit=4 帧限制。JointStreamStep 设计方案已给出(单 Step 持两 caller/统一窗长注入/等齐才发/audio 为句长基准),待用户确认三个配对语义后实施。
- **stream TTS/Motion 加句级 SoS/EoS 包络(与 LLM 轮协议同构)**:stream 模式下每句块流 = 句级 SoS(携带 pass_data 包)→ 块×N(全同形:载荷+timestamp)→ 句级 EoS(下游由此确知块流结束,参照 OpenAI 两 API 的共同选择"结束必须显式");每句 meta 从首块移到句级 SoS,首块可辨性问题消失;cancel 中断不补发 EoS(整轮已作废)。**内部契约名就用 SoS/EoS**,线上名 config 改名(如 SoS→tts_SoS)。配套:①emit 契约条件化——`emitted_signals(config)` classmethod(与 required_catch_signals 同模式),stream 开才要求声明,非 stream 节点声明反而 400;②新增**发出侧线名防撞校验**:emit targets 与 pass targets 相交(如转发轮级 SoS 又原名发句级 SoS)→ 400,消除下游不可辨的同名合流。验证:mock 全协议序精确(SoS 包→双块同形→EoS,改名生效)、validator 11/11、四负例(未声明/非stream误声明/撞名均拒,改名接受)、e2e text+smpl PASS。编辑器限制如实记:MODULE_EMITS 静态表不表达条件契约,stream 节点的 emit 行需手动添加。
- **LLM 流消息瘦身(单进多出协议统一)**:逐句输出与 EoS 只带 timestamp(`ts_only`),每轮 meta(prompt,将来含 pass_vars)只随 SoS 一次性携带——与 stream TTS/motion 的"首块带 meta、后续仅 timestamp"同一原则,机制上封死逐句重复附带。现有 config LLM pass_vars 全空,行为逐位等价;e2e text+smpl 复验 PASS。同时确立**pass 数据搭乘消息的包裹协议**(字段名就叫 `pass_data`,不发明 meta 之类新词):凡随信号/stream 首块传输的 pass_vars 数据,一律包在固定 `"pass_data"` 键下,与信号契约字段(SoS 的 prompt,顶层)和块载荷结构分离;**形状由调用方构造 dict 时直接拼出,emit_signal 保持最简平铺、一字未改**(曾两度试图在 emit_signal 内做包裹机制/fields 参数,被用户指正:调用方本来就手工构造 json,包裹放构造处即可,函数不背机制)。落点:LLM SoS、dispatcher dispatch_start(receiver 相应从 pass_data 包取 base 层)、TTS/Motion stream 首块;后续块仅 timestamp。验证:mock stream 首块结构精确、e2e text+smpl PASS(含 dispatch_start 包裹→receiver 拆包→输出附着全链)。顺带删除 MotionStep 上 KNOWN_CATCH_SIGNALS 死残留;流消息"只带 timestamp"的写法统一为 `output_to_queue(data, pass_data, is_add_pass_data=False)`(机制参数原语义),LLM×2/dispatcher 手拼的 ts_only 全部删除,emit_signal 调用一律由调用方构造完整形状。全量回归:套件 17/17、e2e 六 config 全 PASS、webrtc 四信号 0 错、长对话+cancel ALL PASS、编辑器三套全绿;test_all_configs.py 的 WS 连接加 max_size=None(websockets 库默认 1MB 单帧上限会掐断大合并消息——mime 长句时音频+150帧motion 超线,属测试客户端限制非 server 问题)。双流合并 module(单 Step 持两 caller、节点内配对、单进多出)方向已与用户确认,待定三个配对语义(对齐窗/时长不对称/快慢不对称)。
- **校验逻辑归位到类(validator 退化为纯调度)**:静态检查机制全部搬进 step 类——base 的 classmethod `validate_config(config)` 承载通用检查(声明格式一对一、catch==required 恰好、required inputs、emit==EMIT_SIGNALS 恰好;catch 契约拆为可覆写的 `_validate_catch_contract`),DispatcherStep 经 super() 扩展自己的结构检查(next_nodes/dispatch_vars/dispatch_signals 双向恰好)并覆写 catch 例外;`utils/pipeline_validator.py` 只剩 40 行纯调度(遍历节点→get_class→cls.validate_config,加 node 定位前缀)。dispatcher `custom_init` 里与 validator 重复的四段弱检查删除(引用检查类内零残留)。时机不变:仍在 init_pipeline 建线之前全量拦截(400),零构建成本;classmethod 纯静态(config+类契约,无实例无服务)。验证:行为+校验套件 16/16(全部正负例经新架构)、11/11 config、e2e text+smpl PASS。report validator 段同步。
- **prompt 回显合并进 SoS**:LLM 不再单发回显消息,SoS 信号消息直接携带本轮输入 `prompt` 字段——**固定字段名,不参与 vars 重命名**(信号携带的字段属于信号自身契约);信号 relay 副本保留全部字段,prompt 随 SoS 逐跳到 client(webrtc 链上随 SoS 按组序 interleave 经 DataChannel 下发,实测 `{"signal":"SoS","prompt":...}` 到达)。配套退役:9 个 LLM 节点的回显边(next_nodes 第二条 [x,-1]/[4,7] → [x])与 output_vars 的 prompt 条目全部删除;splitter docstring 中回显例子更新。验证:validator 11/11、SoS 带 prompt 且无独立回显消息(WS 实测)、全量 e2e PASS(text/default/smpl/live/dev_smpl/dev_mime;default 第三次间歇 FAIL 复核仍为 LLM 内容随机——log 零错误、4_action 空串、重跑即过)、webrtc 真实链路 0 错误。
- **信号协议最终形态(运行时+校验+编辑器,全绿)**:经多轮设计收敛,最终协议为——①**条目格式**:catch/pass/emit 一律显式 `{"source","target"}` 双字段(全同名也写全;无字符串简写、无缺省 target,validator 拒为格式错误);每列表**一对一**(source 唯一、target 唯一,多对一合流被拒)。②**运行时**:四态(catch=消费/catch+pass=先 process 后 relay/pass=转发/未声明=error+drop);转发与发射副本**沿边定址**——与数据输出一样定址 `next_nodes[0]`(废除无地址广播;client 的显式边表定址与 server 由此同构);emit 走 `emit_signal(内部名)`,未声明 error+不发;dispatcher 的 pass 经 `_relay_signal` 钩子定向 receiver(折叠 hack 退役,pass-only 不再进 process),**先 catch 才能 dispatch**:dispatch_signals 为字符串列表的列表,引用 catch target 名,每分支订阅哪些 caught 信号(同名可发多分支),process 里逆序定向。③**validator 恰好匹配**:catch targets == required_catch_signals(config)(双向,多/少均 400;KNOWN/CATCH_ANY 等宽容层全部删除);emit 声明 == EMIT_SIGNALS(双向);dispatcher 例外:catch 契约 = dispatch_signals 引用集(双向恰好);列表格式/唯一性校验。④**编辑器同步**:严格解析(仅显式双字段);流推导沿 firstEdge(连线首边,与运行时定址一致)+ dispatcher 定向跳转 + consume-then-relay 节点内弧 + 未声明红线;emit 契约行(MODULE_EMITS,source 锁定 target 可改);**信号拖线**(emit/pass 行出点拖到下游 catch/pass 行入点=设置其 source,纯输入捷径,画线仍由声明推导)。验证:行为套件 16/16(四态/定址/dispatcher 双分支订阅/9 类负例);validator 11/11;e2e 全部 config PASS——text/default/smpl/**live(danmaku)**/dev_smpl/dev_mime(default 与 text 各有一次 LLM 内容随机导致的 flaky,重跑即过、log 零错误);webrtc 真实链路四信号按序 0 错误;编辑器 flow 11 config 0 误报、roundtrip 11/11、Playwright DOM 18 项全过(含拖线用例)。report §Signals 四段改写(显式协议/一对一/沿边定址/dispatcher catch-then-dispatch/校验清单)PDF 通过;README 中英 bullet、CLAUDE.md 约定同步。

- **全部 config 显式补齐 `emit_signals`**(用户指正:emit 的线上名是可配置项,发送侧声明与 catch/pass 同属"信号全部写在 config"原则,不能因缺省同名就不写):脚本按模块类属性 EMIT_SIGNALS 动态补入 14 处——9 个 call_openai_llm(SoS/EoS)、4 个 call_dispatcher(dispatch_start/end)、1 个 audio_collector(recording_start/end),全同名显式 `{source,target}`,键位插在 pass_signals/catch_signals 之后(无则 output_vars 后)。验证:与备份逐 config 对比仅新增 emit_signals 键、validator 11/11、e2e(text+smpl)PASS、编辑器数据往返 OK。行为逐位等价(同名映射 = server 默认)。编辑器侧同步的契约展开(MODULE_EMITS 表、emit 行 source 锁定 target 可改名、导出全写)已实现并语法完整,进一步的流可视化按用户指示暂停。
- **webui 主界面新增 Download 按钮**(Configuration Management 区,复用 /api/config 读取端点,blob 下载 {名字}.json)——补齐"服务器 config → 本地文件 → 编辑器 Load"的闭环;主页 Jinja2 每请求渲染,8001 无需重启即生效。(编辑器信号支持的最终形态见上条。)
- **cancel 端点语义对齐 + init_pipeline fallback 退役**:①report 的 cancellation 形式化(定义/design/send-queue/proof 四处)从"丢弃 ≤ T"改为"丢弃 < T",与全部实现一致(server base/span/splitter/send_data 与 client 各处判断均为严格 <)——代码不动:严格 < 是设计,端点含否由发出方 T±ε 表达(client barge-in 用 -1ms 偏移避免误伤同刻新事件,server 断链清理用 +ε 包含末轮);report 中此约定仅保留一句概念性说明(具体 1ms 等实现常数按用户意见不写入)。②init_pipeline 找不到 config 名不再静默 fallback 到 demo.json(与 400/503 错误语义体系矛盾,且 demo 挂掉时误导排障),改为 404 + config 名明细;实测:未知名 404、合法名 200 不受影响;README 中英错误码说明补 404。PDF 重编译通过。
- **report §Client Architecture 对齐客户端信号改造**(客户端信号系统由用户改造完成,核对确认与 server 同构):镜像段补齐信号内容——四态声明路由(catch/pass 为 scene 序列化字段 = client 的"config",consume-then-relay 同序保证,未声明 error+丢弃)、**client 无改名**(链短单实例,线名即契约名,设计取舍)、RequiredCatchSignals 模块契约 + init 模块局部校验(fail-fast → pipeline_error,对应 server 400/503)、网络模块 catch 列表 = per-scene export manifest(哪些客户端信号跨界进远端管线)、pipeline_ready 穿链自检;模块列举更新(recording/content/action/audio/WS+WebRTC,VAD 移出——它在管线外由 CaptureController 注入信号);§Signals 开头 recording_* 描述从 "voice activity detected" 修正为录音跨度边界。核对中确认的 client 实现细节:四态在 ProcessingModuleSynchronous.TryProcess(catch 先 process 后 relay ✓),pass 转发为显式边表定址(server 是 strip-destination 广播,语义等价逐跳);AudioModule 特例(pass-only+播放门控自然压住 EoS);状态机信号名(listening_*/answering_end/cancel)与 report 原文一致未动。另修正 client 段一处过时描述:原文称 client 内置"SMPL-H motion player 做骨骼重定向"——实际 client 零 SMPL 代码(全工程 grep 0 命中),重定向在 server 侧(smplh_to_humanoid,humanoid_output 默认开),client 的 HumanoidMotionPlayer 只消费预重定向的 humanoid 格式(HumanBodyBones 逐骨旋转+root motion,帧缓冲+crossfade+idle 续填)。PDF 重编译 0 错误。
- **信号系统全面复查(代码+config 静态 & 运行时,全部通过)**:①模块层逐文件复核(Base/Span 两 run 四态+consume-then-relay、Dispatcher 定向 pass→receiver/dispatch_signals、Receiver 括号、Splitter 双入口四态、Collector 手工重发顺序)逻辑一致;②validator 11/11 config 通过,信号声明拓扑逐 config 人工核对(SoS/EoS 链式 pass、webrtc 的 recording_*/connection_start、mime_actress 的 branch_reset 改名)全部闭合;③历史+新增 client log 全量 grep:**0 条 undeclared signal**;④实弹 e2e:text/default/smpl/live 四 config PASS(SoS→逐句→EoS、dispatcher/receiver 合流),**webrtc 真实网关链路首次实测通过**(客户端按序收到 recording_start/recording_end/SoS/EoS 各一,connection_start 被 splitter 消费启时钟),mime_actress 信号层全对(branch_reset 定向改名实弹验证:motion 节点收到并按 catch 改名为内部 SoS)。⑤顺手修复 server_fastapi.py init_pipeline 处过期注释(仍描述已否决的跨模块 closure 语义→改为 per-node 描述)。⑥自包含收尾:technical report 的 pipeline config 示例补 `pass_signals`(原示例无信号声明,按四态规则照抄会 drop SoS/EoS,与正文矛盾),§Data Routing 机制列表补信号键条目(指回 §Signals),PDF 重编译 0 错误;test_all_configs.py 维持正式 config 范围不变(dev_* 为开发实验配置,不在 sync 范围;曾误加后按用户指示撤回)——正式在册:demo+unity_chan 四链,webrtc 走专用 test_webrtc.py。**发现的非信号问题**:demo config 用官方 openai ASR 账号配额 429(init 503 fail-fast 机制借此实弹验证);dev_mime_actress_smpl 与 dev_smpl 的 motion `duration: 0.0` 被远程 18084(HYMotion 格式)拒绝——`duration 0.0 out of range (0, 120]`,motion 分支全空;**已解决:两 config duration 改为 5.0,复测双双 PASS**(dev_smpl 收到 audio_data+action;mime 8 段 motion 各 150 帧,branch_reset 定向改名链 log 逐跳确认,0 error。**勘误**:当时记的"continuous history 续接正常"是误报——flood web_demo 服务端明确忽略 is_continuation/history 字段(MotionRequest extra=ignore,文档 Ignored 列表),caller 的 continuous 是面向支持续接协议的 motion 服务的通用能力,在 flood 后端上不生效也不报错;use_prompt_engineering 同在忽略列表,此前"开关无差"的正确解释是字段不生效)。
- **节点 init 错误检测(fail-fast,静默降级退役)**:此前依赖服务挂掉时三层吞错(caller `_init_call` try/except、`_init_with_timeout` 超时/异常仅 log、端点照返 200),管线半残废只能翻 log。现在:①LLM/TTS/ASR 的 `_init_call` 失败**抛出**(warmup 失败即 init 失败——语义取舍:失去"暂时挂稍后自愈"的宽容,部署顺序变硬约束:先起模型服务再 init 管线);②base `_init_with_timeout` 把超时/异常记入 `self.init_error`;③`setup_processing_pipeline` 每建一个节点即查(节点构建本就串行),**第一个失败立即中止**(最坏阻塞从 n×60s 缩到单节点 60s),已起线程 kill+join+状态复位(不能走 dispose——它有 initialized 守卫);④端点返 **503**+节点定位+错误明细(与静态校验的 400=配置错区分:503=环境错,可重试)。验证:base 异常/超时记录、坏依赖 config 端点 503(0.0s 即返,fail-fast)、**失败后同 client 重试合法 config 200**(清理完备可复用)。

- **信号系统补全:emit 声明 + init 静态校验(信号/vars 双闭合,error 级拒启动)**。
  - **`emit_signals`(发送侧,补齐三面对称)**:模块以类属性 `EMIT_SIGNALS` 声明"会发出的内部信号名"(LLM=SoS/EoS、AudioCollector=recording_*、Dispatcher=dispatch_start/end),发送统一走 `base.emit_signal(内部名, pass_data, **kw)`,线上名由 config `emit_signals`(`{"source","target"}`,缺省同名)映射——**发出名可改是嵌套 dispatcher/receiver 可接线的功能前提**(内层包络改名与外层隔离,mock 验证:inner_ds/inner_de 被内层 receiver 全 catch 零外泄)。
  - **dispatcher 语义修正**:其 `pass_signals` 是**定向 pass 给 receiver**(逻辑主干;物理穿过分支,分支零声明零感知),不是沿链广播——之前让 base 沿物理链 relay 是错的;pass-only 声明被内部折叠进 catch 以进 process。定向信号到达目标后四态照常(仅 pass=接力转发去 destination,receiver 由此把 SoS/EoS 续传下游)——撤销了此前自加的"定向 pass 无意义"限制。receiver 处每信号恰一份(测试断言 count==1;三个重复源头各自堵死:原广播被 dispatcher catch 折叠消费、主干 pass 恰一条、dispatch_signals 副本被分支 catch 终结)。
  - **`utils/pipeline_validator.py` + init_pipeline 接入(校验哲学:纯节点局部,不做跨模块流建模)**:构建前静态校验,任何 finding → 400(明细进响应体+client log)。首版做了全链传播模拟(推导信号路径逐节点核对+vars 沿链可用集,曾实测 11 config 0/0),但被指出该做法在校验器里**复刻各模块路由语义**(dispatcher/receiver 都要特判建模,且确实修过一次 receiver pass_vars 改名的模型缺口)——重写为**模块自洽契约**:各模块以类属性声明自身需求(`REQUIRED_CATCH_SIGNALS`/`REQUIRED_INPUTS`,条件契约用 classmethod——如 motion 的 continuous→SoS),校验只查"每个节点的 config 满足它自己模块的契约"(required catch ⊆ catch **targets**——改名接线仍满足契约,因 handler 认 target 名;required input ⊆ input_names;节点内引用一致性:emit_signals source ⊆ EMIT_SIGNALS、dispatcher 的 dispatch_vars/signals ⊆ 自身 output/catch targets)。**跨节点断链不静态查,由运行时四态兜底**(未声明信号在第一个到达节点被拦+client log 报错)。顶层 `client_fields`/`client_signals` 随传播模拟一并撤销(它们是给模拟喂外部源的,且"漏写"无参照可核对——外部世界本就是声明式校验的边界)。11 config 局部校验全过;四类契约缺失用例(receiver 无 catch/TTS 无 text input/continuous motion 无 SoS/emit 引用不存在的信号)全部被抓 → 400。
  - **E2E 终验**:unity_chan_default 与 unity_chan_smpl(dispatcher 管线)真实语音全流程 SoS→内容→EoS 照常。(此前记录的"motion 字段缺失"是误报:e2e 脚本按 'motion' 字符串找字段,该链路 motion 数据在 `action` 字段——client log 证实 150 帧 humanoid 正常产出;LLM 短句无 [action] 标记时 motion 为空是正常行为。)
- **`dispatch_vars` 对齐 target 名**(与 dispatch_signals 同规则):引用 output_vars 改名后的 target(线上名,如 `4_action`)而非 output_name;DispatcherStep 建 target→output 反查+构造期校验(引用非 target 报错),4 个 dispatcher config 迁移(`action→4_action`、`text→4_audio_text`,与分支 input source 声明恰好一致——旧机制发出键本就是 target,行为等价),校验器同步(dispatch_vars 直接即分支可用集+非 target 告警),11 config 复检 0/0,unity_chan_smpl e2e 复验通过。

- **信号系统重构:全 config 声明 + 四态路由,漂流退役**。信号与数据消息完全同规则(带内逐跳、不超车、不漂流),每个到达节点的信号**必须**在该节点 config 声明,否则 error+丢弃。声明与 vars 同构且支持改名(显式 `{"source","target"}`,默认全同名,排列紧跟 vars 区):`catch_signals`(交给 process,按 target 改名——模块内部信号名与链上名解耦)/`pass_signals`(改名转发)。四态:仅 catch=消费终结;catch+pass=**consume-then-relay**(process 返回后放行,节点因信号产生的输出恒在信号前);仅 pass=转发;未声明=接线错误。定向信号(带 destination,如 dispatch_*)中间节点穿透、目标必须 catch(pass 对定向无意义,忽略+报错);广播信号(无 destination,如 SoS/EoS)逐跳凭 pass 声明前进;cancel 独立带外(唯一无序通道,非信号)。
  - **代码**:Base/Span 两个 run 的信号分支重写(四态+改名);7 处模块硬编码 `catch_signal_set` 全部删除(Receiver/AudioCollector/FrameSplitter/Memory/Motion×2/Danmaku,docstring 注明各自需要的 config 声明);LLM(openai+base)的 SoS/EoS、AudioCollector 重发的 recording_* 显式广播化(`is_add_destination=False`);MemoryManager/MotionStep 的手工重发改为框架 pass;**AudioCollector 保留手工重发**(它需要 recording_end 在 WAV 之前的自定义顺序,catch-only+自行再发送是合法模式);splitter 两个信号入口对齐四态。
  - **DispatcherStep 新增 `dispatch_signals`**(与 dispatch_vars 平行的列表的列表):catch 到的信号(按改名后 target)重发为**定向**信号给指定分支;配合 catch 改名使分支定向名 ≠ 链上广播名,防双触发;引用非 catch target 的名字构造时报错。
  - **11 个 pipeline config 按信号流注入声明**(脚本推导:SoS/EoS 自 LLM 位置向下游、recording_* 自 collector 重发向下游、connection_start 自 q0 至 splitter 终结、playback_complete 至 danmaku 终结;cont-motion catch+pass SoS;dispatch_* 定向不需途经声明),键序重排至 vars 区。
  - **验证**:mock 四态/改名/定向命中/未命中丢弃/穿透不被截/dispatcher 分发(定向触达分支+广播恰一次);unity_chan_webrtc 全链 7 节点信号流模拟(connection_start 被 splitter 终结,recording/SoS/EoS 有序到出口);真实模块回归(Receiver 组合并、AudioCollector 顺序、MemoryManager 放行);**生产 8910 端到端两轮**(unity_chan_default 真实语音→ASR→gemma→TTS:SoS→2 文本+2 音频→EoS 完整有序)。technical report §Signals 改写为"declared routing, no drifting"(四态/双原语/dispatch_signals/cancel 反例定性),vad_* 旧名全部改 recording_*,PDF 重编译通过。

- **TTS / Motion 模块新增 `stream` 配置选项（仅改 module,未动任何 config)**:节点 config 加 `"stream": true` 后,`process` 改为每个 chunk 一条消息:首条带全部 pass_vars meta(下游每句恰好见一次 meta,与非流式一致),后续仅带 timestamp(cancel 语义对每 chunk 生效);chunk 间 `check_cancel`。**缺省(不加 stream)行为逐字节不变**。基类 `BaseTTSCaller/BaseMotionCaller` 提供 `call_stream` 退化实现(单 chunk=整段 call 结果),任何 caller 开 stream 都能跑。
  - **OpenaiTTSCaller.call_stream**:走 **SSE 端点**(`extra_body={"stream_format":"sse"}`,逐行解析 `speech.audio.delta` 的 b64 pcm16,采样率取 `X-Sample-Rate` 头,done 事件自然收尾),`_rechunk` 重缓冲成 `stream_chunk_ms`(默认 300,强制 100ms 倍数——splitter 20ms 帧×5/组无缝打包,非尾 chunk 零 pad)的 WAV 块。**_rechunk 不添加任何字节**(单测 join≡原始流),补零只发生在 splitter 对句尾短尾块(<120ms,落在句尾自然静音)。坑:服务端一次 flush ~900ms,必须逐 chunk_bytes 切开 yield(首版整段 yield 导致 900ms 大块)。SSE 版实测:15 chunks 恰 300ms、首块 242ms(与 pcm 路径同)、14 个 chunk 边界零静音注入、ASR 回环逐字全对。
  - **MotionGenerationCaller.call_stream**:走 flood `/api/generate_json_stream`(SSE),`stream_size` config 可设;humanoid 模式**增量转换**——`smplh_to_humanoid` 新增 `prev_trans/ref_y` 可选参数(缺省=原整段行为逐位不变):prev_trans 使 chunk 首帧 root_xz 为真实步长、ref_y 钉住会话首帧骨盆 Y(消除 chunk 边界髋部跳变);raw 模式转发 smplh chunk(补 framerate,自 `X-Framerate` 头)。continuous 的 history 跨 delta 滚动累计,流结束保存尾 N 帧(与 call() 一致),done 事件取 betas。
  - **验证全过**(直接构造对象/mock queues,QwenTTS 8011 + flood 18085 真机):重切块单元(不规则流无损保序)/流式 chunk 恰 300ms+首块 243ms+**流式与非流式 ASR 回环逐字全对**(QwenTTS 无 seed,两次生成不可逐位比,改内容级验证)/**增量 humanoid 拼接 ≡ 整段转换逐位一致**(root_xz/hips_pos/52 骨 joints max diff=0,12 chunks/93 帧)/step 消息级首条含 meta 后续不含/非流式两模块回归单消息字段不变/continuous+stream 的 history 保存与续发。

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
