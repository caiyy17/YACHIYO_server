# YACHIYO Server — 项目须知

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
