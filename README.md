# YACHIYO Server

A modular, streaming pipeline server for real-time AI assistant applications. Supports voice input, LLM conversation, motion generation, and voice output, with WebRTC and WebSocket interfaces. For detailed technical analysis including formal proofs, latency models, and benchmarks, see the [technical report](technical_report/main.pdf).

## Quick Start

```bash
conda activate yachiyo  # see requirements.txt for dependencies

# Main server (pipeline)
uvicorn server_fastapi:app --reload --host 0.0.0.0 --port 8910

# WebRTC server (bridges WebRTC clients to pipeline)
python server_webrtc.py --port 15168 --main-server http://localhost:8910
```

## Standalone Model Services

The pipeline server itself is lightweight — it only orchestrates message routing and calls external model services via HTTP. All compute-heavy models run as **standalone services** in their own environments:

| Service               | Directory                            | Description                                      | License                                                               |
| --------------------- | ------------------------------------ | ------------------------------------------------ | --------------------------------------------------------------------- |
| ASR (Qwen3-ASR)      | `Modules_standalone/QwenASR/`        | OpenAI Whisper-compatible wrapper for Qwen3-ASR   |
| LLM (vLLM)            | `Modules_standalone/VLLM/`           | Config files for vLLM's native OpenAI API        | [Apache 2.0](https://github.com/vllm-project/vllm)                    |
| TTS (Qwen3-TTS)      | `Modules_standalone/QwenTTS/`        | OpenAI TTS-compatible wrapper for Qwen3-TTS       |
| MotionGen (HY-Motion) | `Modules_standalone/HYMotion/`       | REST API wrapper for text-to-motion generation   | [Hunyuan Community](https://github.com/Tencent-Hunyuan/HY-Motion-1.0) |
| Vector Database       | `Modules_standalone/VectorDatabase/` | BGE-M3 + FAISS similarity search server          | MIT / Apache 2.0                                                      |

Each service has its own conda environment, setup instructions, and README. The pipeline server connects to them only through HTTP addresses configured in `configs/settings/settings.json` — no code or import dependency.

**You can replace any service** with any implementation that exposes the same OpenAI-compatible API (e.g., swap any service with any implementation that exposes the same OpenAI-compatible API.

## Architecture

```
Client (WebRTC / WebSocket)
  |
  v
server_webrtc.py (port 15168)     <-- WebRTC bridge (optional)
  |  WebSocket
  v
server_fastapi.py (port 8910)     <-- Pipeline server
  |
  v
Pipeline: [Node 1] -> [Node 3] -> [Node 5] -> [Node 7] -> Client  (example)
           ASR        LLM         RAG/Query    TTS
```

- **server_fastapi.py**: Pipeline server. Manages client registration, pipeline initialization, and message routing via WebSocket.
- **server_webrtc.py**: Pure WebRTC bridge. Converts between WebRTC tracks and pipeline WebSocket messages.

## Pipeline Configurations

| Config              | Pipeline                                                     | Description                                  |
| ------------------- | ------------------------------------------------------------ | -------------------------------------------- |
| `default`           | Dispatch → FuncA ∥ FuncB → Receive                           | Parallel test pipeline                       |
| `demo`              | ASR → LLM → TTS                                              | Minimal conversation (OpenAI API)            |
| `unity_chan`        | ASR → LLM → DataQuery → TTS                                  | Conversation with RAG action matching        |
| `unity_chan_openai` | ASR → LLM → DataQuery → TTS (all cloud)                      | Same as above, using OpenAI API              |
| `unity_chan_webrtc` | AudioCollector → ASR → LLM → DataQuery → TTS → FrameSplitter | WebRTC frame-level streaming                 |
| `unity_chan_smpl`   | ASR → LLM → Dispatch → MotionGen ∥ TTS → Receive             | Free-form SMPLH motion generation (parallel) |

### Node Types

| Module              | Function Name                       | Description                                                          |
| ------------------- | ----------------------------------- | -------------------------------------------------------------------- |
| `asr_openai`        | `call_openai_asr`                   | Speech-to-text via OpenAI-compatible API                             |
| `llm_openai`        | `call_openai_llm`                   | Streaming LLM with history, lorebooks, tool calls, action extraction |
| `data_query_link`   | `call_data_query_link`              | RAG-based action matching via BGE embedding                          |
| `motion_generation` | `call_motion_generation`            | Text-to-motion via HY-Motion API, returns SMPLH params               |
| `tts_openai`        | `call_openai_tts`                   | Text-to-speech via OpenAI-compatible API                             |
| `parallel`          | `call_dispatcher` / `call_receiver` | Fork-join parallel execution bracket                                 |

## Workflow

We restrict the input workflow to satisfy the following conditions:

1. It is a directed acyclic graph (DAG)
2. It has exactly one entry and one exit
3. It is consistent with depth-first scheduling order
4. Each node's output depends only on messages it has already received

We can prove that such a workflow can be written as a linear pipeline, i.e., nodes connected sequentially from entry to exit. The proof is as follows:

1. We can always find a topological ordering where every node connects to a node with a larger index; we arrange them in this order
2. When a node produces output, it tags the message with the destination node's index
3. When the message reaches the next node, it simply checks whether it is the target node; if not, it forwards the message

The downside of this approach is: each message is blocked behind the previous message's processing tail (e.g., if the previous message is still being processed by node A, the current message cannot be processed by any node, even those parallel to A). This is because even parallel nodes are forced into a topological order. For independent nodes that both need to process the same message, swapping their order does not reduce the combined latency — the total is always the sum of all processing times regardless of ordering.

This serialization overhead can be eliminated using the **dispatcher-receiver bracket**: the dispatcher emits branch messages in reverse topological order so that later nodes' messages are forwarded through earlier nodes before those nodes begin processing, enabling concurrent execution. See `Modules/parallel/` for the implementation.

For convenience, we allow control signals to be sent directly to the client independently of the data channel — effectively an express lane, though it can only deliver to the final output and is generally used only for status monitoring.

## Configuration

In general, we can manually retain only the information useful for downstream node processing, to save transmission bandwidth. When the full pipeline graph is known, each node can determine which information to keep and which to discard. We therefore let each node choose which historical information to retain. The pipeline configuration scheme is as follows:

1. The processing function for each node, in topological order
2. Input variables mapped to all preceding nodes' output variables (with node index prefixes)
3. Possible output destination node indices
4. Other node-specific configuration

For example, the configuration for node 5 might look like:

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

When this node produces output, it includes its own node index and the destination in the message, for example:

```json
{ "destination": "11", "5_output_xxx": "example_xxx", "5_output_yyy": "example_yyy" }
```

Note that the system's original input is considered node 0, so numbering starts from 1. The final node's output omits the node index prefix, and node 1's input corresponds to client-provided variables. The pipeline supports non-contiguous numbering as long as indices are monotonically increasing. In the implementation, messages without a destination or with destination -2 are processed by the immediately next node; destination -1 is processed by the final node.

## Signals

For flexibility and cleaner configuration, we define a `signal` as a reserved parameter that is automatically extracted from preceding messages. Each node can declare which signals it catches. For example, if LLM node A is connected to downstream node B, A emits an EoS signal and B catches it — this is built-in logic that does not need to be specified in the configuration. (Therefore, all nodes must ensure signal naming is consistent and distinct, so that signals intended for processing are caught and others are forwarded.)

If a signal reaches a node that does not catch it, the signal's **destination is removed and it is forwarded directly to the next node**. Note the routing behavior of signals — if necessary, a dedicated node can be created to handle or discard specific signals. The rationale is: if you want a signal to reach the client directly, simply emit it; as long as no downstream node explicitly catches it, it will propagate all the way to the client.

## Timestamps

To handle interruptions (barge-in), every message carries a timestamp, set when the server receives the message. The timestamp is propagated unchanged through all pipeline stages (timestamp is a reserved parameter that does not need manual handling, though each node retains the right to modify it). When an interruption signal is received, every node receives the cancel signal and its timestamp via a side channel (bypassing the queue). Nodes subsequently discard all messages with timestamps earlier than the latest cancel timestamp.

Processing nodes check the timestamp when they begin processing a message, and the final send node checks before transmission, ensuring the server never sends a cancelled message.

The timestamp preferentially uses the client-provided value; if the client does not provide a timestamp, the server uses `time.time()` as a fallback.

## LLM Module

For the LLM, we use a modular rather than pipeline-based approach, with a HistoryManager responsible for:

1. Loading conversation history
2. Modifying history based on settings
3. Saving history in the specified format

StreamCutter segments the LLM's streaming output into individual sentences and attaches specified formatting (e.g., extracting actions and tags).

The LLM module handles interaction with the language model.

## LoreBooks

We use LoreBooks to configure character information, with the following format:

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

Supported activation strategies: `constant` (always active), `keywords` (keyword matching), `vectorized` (vector similarity), `both`. Entries are injected at their specified positions in the conversation history.

## Client Flow

```
1. POST /register/                          -> Register client
2. POST /init_pipeline/{client_id}          -> Load pipeline config
3. WS   /ws/{client_id}                     -> Connect WebSocket
4. Send: {"text": "...", "audio_file": "base64...", "timestamp": 123.45}
5. Recv: {"text": "...", "audio_data": "base64...", "action": "...", "action_hint": "..."}
6. POST /unregister/                        -> Cleanup
```

### WebRTC Client Flow

1. Register + init pipeline on main server (same as above)
2. `POST /offer/{client_id}` on WebRTC server for SDP exchange
3. Send/receive data via audio/video tracks and DataChannel
4. Audio (48kHz, 50fps) and video (30fps) are grouped into 100ms synchronized frame groups by FrameSplitter/GroupDispatcher
