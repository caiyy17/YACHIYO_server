# YACHIO Server

实时 AI 助手的模块化流式 pipeline 服务器。支持语音输入、LLM 对话、动作生成和语音输出，提供 WebRTC 和 WebSocket 两种接口。详细的技术分析（包括形式化证明、延迟模型和性能测试）请参阅 [technical report](technical_report/main.pdf)。

## 快速开始

```bash
conda activate yachio  # 依赖见 requirements.txt

# 主服务器（pipeline）
uvicorn server_fastapi:app --reload --host 0.0.0.0 --port 8910

# WebRTC 服务器（桥接 WebRTC 客户端到 pipeline）
python server_webrtc.py --port 15168 --main-server http://localhost:8910
```

## 独立模型服务

Pipeline 服务器本身非常轻量——它只负责消息路由编排，通过 HTTP 调用外部模型服务。所有计算密集型模型作为**独立服务**运行在各自的环境中：

| 服务                  | 目录                                 | 说明                                      | 协议                                                                  |
| --------------------- | ------------------------------------ | ----------------------------------------- | --------------------------------------------------------------------- |
| ASR (Qwen3-ASR)      | `Modules_standalone/QwenASR/`        | Qwen3-ASR 的 OpenAI Whisper 兼容 wrapper |
| LLM (vLLM)            | `Modules_standalone/VLLM/`           | vLLM 原生 OpenAI API 的配置文件           | [Apache 2.0](https://github.com/vllm-project/vllm)                    |
| TTS (Qwen3-TTS)      | `Modules_standalone/QwenTTS/`        | Qwen3-TTS 的 OpenAI TTS 兼容 wrapper     |
| MotionGen (HY-Motion) | `Modules_standalone/HYMotion/`       | 文本到动作生成的 REST API wrapper         | [Hunyuan Community](https://github.com/Tencent-Hunyuan/HY-Motion-1.0) |
| 向量数据库            | `Modules_standalone/VectorDatabase/` | BGE-M3 + FAISS 相似度搜索服务             | MIT / Apache 2.0                                                      |

每个服务有独立的 conda 环境、部署说明和 README。Pipeline 服务器仅通过 `configs/settings/settings.json` 中配置的 HTTP 地址连接它们——无代码或 import 依赖。

**可以替换任何服务**为任何暴露相同 OpenAI 兼容 API 的实现（例如用 Ollama 替换 vLLM，或用其他 OpenAI 兼容实现替换），只需修改配置文件。

## 架构

```
客户端 (WebRTC / WebSocket)
  |
  v
server_webrtc.py (端口 15168)     <-- WebRTC 桥接（可选）
  |  WebSocket
  v
server_fastapi.py (端口 8000)     <-- Pipeline 服务器
  |
  v
Pipeline: [节点 1] -> [节点 3] -> [节点 5] -> [节点 7] -> 客户端  (示例)
           ASR        LLM         RAG/Query    TTS
```

- **server_fastapi.py**：Pipeline 服务器，管理客户端注册、pipeline 初始化和 WebSocket 消息路由。
- **server_webrtc.py**：纯 WebRTC 桥接，在 WebRTC 媒体轨道和 pipeline WebSocket 消息之间转换。

## Pipeline 配置

| 配置                | Pipeline                                                     | 说明                            |
| ------------------- | ------------------------------------------------------------ | ------------------------------- |
| `default`           | Dispatch → FuncA ∥ FuncB → Receive                           | 并行测试 pipeline               |
| `demo`              | ASR → LLM → TTS                                              | 最小对话（OpenAI API）          |
| `unity_chan`        | ASR → LLM → DataQuery → TTS                                  | 对话 + RAG 动作匹配             |
| `unity_chan_openai` | ASR → LLM → DataQuery → TTS（全云端）                        | 同上，使用 OpenAI API           |
| `unity_chan_webrtc` | AudioCollector → ASR → LLM → DataQuery → TTS → FrameSplitter | WebRTC 帧级流式传输             |
| `unity_chan_smpl`   | ASR → LLM → Dispatch → MotionGen ∥ TTS → Receive             | 自由形式 SMPLH 动作生成（并行） |

### 节点类型

| 模块                | 函数名                              | 说明                                                 |
| ------------------- | ----------------------------------- | ---------------------------------------------------- |
| `asr_openai`        | `call_openai_asr`                   | 通过 OpenAI 兼容 API 进行语音识别                    |
| `llm_openai`        | `call_openai_llm`                   | 流式 LLM，支持历史记录、lorebook、工具调用、动作提取 |
| `data_query_link`   | `call_data_query_link`              | 基于 BGE embedding 的 RAG 动作匹配                   |
| `motion_generation` | `call_motion_generation`            | 通过 HY-Motion API 生成 SMPLH 动作参数               |
| `tts_openai`        | `call_openai_tts`                   | 通过 OpenAI 兼容 API 进行语音合成                    |
| `parallel`          | `call_dispatcher` / `call_receiver` | 分发-接收并行执行括号                                |

## 工作流

我们限定输入的工作流符合以下条件：

1. 是一张有向无环图（DAG）
2. 有且仅有一个入口，一个出口
3. 与深度优先调度顺序一致
4. 每个节点的输出仅和这个节点已经接收到的消息有关

我们可以证明，这样的工作流可以被写成一个线性流程，即若干个节点从入口开始首尾相接直至出口，证明如下：

1. 我们必定可以找到一种拓扑排序，使得每一个节点必定连向比他编号更大的节点，我们将其按照拓扑排序排成一排
2. 每个节点输出的时候，我们可以给请求打上标记，表明这个节点要输出至的节点编号
3. 这个消息传到下一个节点时，只需检测是否为目标节点，如果不是就跳过

这样做的一个弊端是：每个消息会被阻塞在上一个消息处理的最末尾（比如说上一消息的最末尾在被 A 处理，那么这个消息的任何部分都不会被先与 A 的节点处理，哪怕有些节点可以是和 A 并列的）。原因是两个节点即使是并列的，我们也给他强行规定了拓扑顺序。对于都需要处理同一消息的独立节点，交换顺序不会降低总延迟——总时间始终是所有处理时间之和，与顺序无关。

这个串行化开销可以通过**分发-接收括号**消除：分发节点按反向拓扑序发射分支消息，使后续节点的消息先通过前序节点的转发路径到达，从而实现并发执行。详见 `Modules/parallel/` 的实现。

为了流程的便利性，我们允许控制通道的信号直接发送给 client 而独立于数据通道，实质上就是超车通道，只不过这个超车只能直接送到最终输出，一般只用作信号监控使用。

## 配置文件

通常情况下，我们可以人为地仅保留会对之后节点处理有用的信息，以节省传输带宽。在完整流程图已知的情况下，每个节点可以知道需要保留哪些信息，所以我们让每个节点来选择需要保留哪些历史信息，丢弃哪些。为此，我们设计了如下的流程图配置方案：

1. 按照排序，每个节点使用的处理函数
2. 输入变量对应的所有之前节点的输出变量（带有节点编号）
3. 可以输出至的节点编号
4. 其他节点相关配置

例如，某个 5 号节点的配置可能如下：

```json
{
    "node_id": 5,
    "function": "call_func_xxx",
    "config": {
        "input_vars": [
            {
                "input_name": "input1",
                "sources": ["2_aaa", "4_aaa"]
            },
            {
                "input_name": "input2",
                "sources": ["2_bbb", "4_bbb"]
            }
        ],
        "pass_vars": [
            {
                "sources": ["2_xxx", "4_xxx"],
                "targets": ["5_output_pass"]
            }
        ],
        "output_vars": [
            {
                "output_name": "output1",
                "targets": ["5_output_xxx"]
            },
            {
                "output_name": "output2",
                "targets": ["5_output_yyy"]
            }
        ],
        "next_nodes": [7, 11],
        "other_settings": "func_xxx_example_settings"
    }
}
```

这个节点在输出的时候会在请求中标明自己的节点标号和目标节点，例如：

```json
{ "destination": "11", "5_output_xxx": "example_xxx", "5_output_yyy": "example_yyy" }
```

注意因为系统的原始输入算是 0 号，所以编号从 1 开始，同时最终节点的输出不加节点编号，1 号节点的输入也是与客户端传入的变量有关。这套 pipeline 也支持非连续编号，只要编号是单调递增的即可。实现中，我们认为没有 destination 的信息或者 destination 为 -2 默认被紧邻的一个节点处理，destination 为 -1 的节点被最终节点处理。

## 信号

同时为了增加灵活性，也为了配置更加简洁，我们定义了一个 signal 的默认参数，这个参数如果存在于前序信息中，它会自动被提取。而每个节点可以写入需要 catch 的 signal 参数，比如语言模型节点 A 接入了处理语言模型输出的节点 B，A 会抛出 EoS 的 signal，B 会 catch 这个信号，因为这是内置的逻辑，不需要在配置文件中写出。（也因此所有节点需要注意 signal 信号的命名统一性和区分性，让需要被处理的信号被接受，不需要被处理的信号能被放行）

如果一个 signal 信息到达某个节点，但是这个节点并不 catch 这个信号，那么这个信号会被**删除 destination 信息后直接被传往下一个节点**，请注意 signal 信号的路由，必要的时候可以专门写一个节点来处理或者遗弃 signal 信号。这么做的原因是：如果我有一个信号希望它能直接被 client 接收，那么直接抛出即可，只要后续没有节点特定要 catch 这个信号，那么就可以一直传到 client。

## 时间戳

为了处理打断，我们给每个信息都加上了时间戳，时间戳为服务器接收到该信息的时间，并且保持不变地一直被传递下去（timestamp 信息也是默认参数，不需要手动处理，但是每个节点也保留对时间戳信息进行加工的权利）。接收到打断信号时，每个节点都会从旁路（跳过队列）直接收到打断信号和相应的时间戳，节点在后续处理时会抛弃所有早于当前接收到的最新打断时间戳的信息。

处理节点会在开始处理信息时检查时间戳，而最终的发送节点（至 client）会在发送前检查，保证服务端不会发送已经被取消的信息。

时间戳优先使用客户端传入的值，如果客户端未传入时间戳，服务端会以 `time.time()` 作为 fallback。

## 语言模型

对于语言模型，这边暂时采用了模块而非流程的处理方案，设置了 HistoryManager 负责：

1. 加载历史记录
2. 根据设置修改历史记录
3. 用指定格式保存历史记录

StreamCutter 负责把语言模型流式输出截断为一个个短句，并附加指定格式（例如提取动作以及标签）

语言模型模块负责和语言模型交互

## LoreBooks

我们使用了 LoreBooks 来配置角色信息，格式如下：

```json
{
    "data": [
        {
            "name": "test_name",
            "strategy": "constant",
            "position": "0",
            "role": "system",
            "order": 0,
            "probability": 1,
            "keywords": ["xxx"],
            "vectorization_keywords": ["yyy"],
            "context_length": 2,
            "threshold": 0.5,
            "logic": "and_any",
            "case_sensitive": true,
            "content": "This is the content"
        }
    ]
}
```

Lorebook 条目支持的激活策略：`constant`（始终激活）、`keywords`（关键词匹配）、`vectorized`（向量相似度）、`both`。条目会在对话历史的指定位置被注入。

## 客户端流程

```
1. POST /register/                          -> 注册客户端
2. POST /init_pipeline/{client_id}          -> 加载 pipeline 配置
3. WS   /ws/{client_id}                     -> 连接 WebSocket
4. Send: {"text": "...", "audio_file": "base64...", "timestamp": 123.45}
5. Recv: {"text": "...", "audio_data": "base64...", "action": "...", "action_hint": "..."}
6. POST /unregister/                        -> 清理
```

### WebRTC 客户端流程

1. 在主服务器上注册 + 初始化 pipeline（同上）
2. `POST /offer/{client_id}` 在 WebRTC 服务器上进行 SDP 交换
3. 通过 audio/video/DataChannel 收发数据
4. Audio (48kHz, 50fps) 和 video (30fps) 由 FrameSplitter/GroupDispatcher 按 100ms 同步组打包/解包
