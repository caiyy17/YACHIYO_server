# YACHIYO Server — 项目须知

## 信号声明约定(最终协议)

- **emit/catch/pass 三面都必须显式写在 config 里**:条目一律为显式 `{"source","target"}` 双字段(全同名也要写全;无字符串简写、无缺省 target)。每列表**一对一**:source 唯一(一个信号只映射一个名)、target 唯一(禁止多对一合流)。
- **validator 恰好匹配**:catch targets == 模块 `required_catch_signals(config)`(多一个少一个都 400);emit 声明 == `EMIT_SIGNALS`(双向)。dispatcher 例外:它自身不消费,其 catch 契约 = dispatch_signals 引用集(双向恰好,"先 catch 才能 dispatch")。
- **信号沿边定址**:转发/发射副本与数据一样定址 `next_nodes[0]`(第一条边);dispatcher 的 pass 定向 receiver(_relay_signal 钩子)。dispatch_signals 是字符串列表的列表(每分支订哪些 caught 信号,按 catch target 名)。

## 配置范围约定

- `configs/` 里 **`dev_*` 开头的 config 是开发实验配置,不在 sync 范围内**:不纳入 `test/test_all_configs.py` 的正式在册列表,文档/测试同步时跳过。正式范围 = demo + unity_chan_* 系列。
- `loopback.json` / `test_frame_splitter.json` 是工具 config,不适用语音 e2e。

## 环境与服务

- conda 环境:`yachiyo`(不要用 `conda run`,会吞 stdout;用脚本文件 + `source ~/miniforge3/etc/profile.d/conda.sh && conda activate yachiyo`)
- 主服务:8910(`uvicorn server_fastapi:app --reload`,代码改动自动重启);WebRTC 网关:15168;本地 QwenTTS:8011
- 远程 motion 服务(HYMotion 格式,如 47.84.79.234:18084)要求 `duration ∈ (0, 120]`,不接受 0

## 测试

- 正式 e2e:`python test/test_all_configs.py [config名...]`(不带参数跑全部在册 config)
- WebRTC 专用:`python test/test_webrtc.py --mode single|lifecycle|multi|framesplitter`
- 客户端日志在 `logs/client_<id>.log`;信号接线错误 grep `undeclared signal`

## 文档

- 系统语义(信号四态/路由/校验)的权威文档是 `technical_report/main.tex`,README 只放概览 bullet
- 改动记入 `PROGRESS.md`(只保留最新版本要求,不堆历史)
