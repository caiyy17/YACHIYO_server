# HY-Motion Server

REST API + Web UI wrapper for [HY-Motion](https://github.com/Tencent-Hunyuan/HY-Motion-1.0), a text-to-motion generation model that produces SMPLH body parameters.

> **License**: HY-Motion is released under the [Tencent Hunyuan Community License](https://github.com/Tencent-Hunyuan/HY-Motion-1.0/blob/master/License.txt). Please comply with its license terms when using this wrapper.

## Setup

1. Clone HY-Motion to a sibling directory:

    ```bash
    cd ..  # parent of YACHIYO_server
    git clone https://github.com/Tencent-Hunyuan/HY-Motion-1.0.git
    cd MotionServer
    ```

2. Create conda environment and install dependencies (see HY-Motion README for details):

    ```bash
    conda create -n hymotion python=3.10
    conda activate hymotion
    pip install -r requirements.txt
    ```

3. Copy the wrapper files into the HY-Motion directory:

    ```bash
    cp ../YACHIYO_server/Modules_standalone/HYMotion/*.py custom/
    ```

4. Download model checkpoints following HY-Motion's instructions.

## Run

```bash
conda activate hymotion
cd MotionServer

# With prompt engineering (uses an LLM to rewrite prompts + estimate duration):
python custom/web_server.py --model_path ckpts/tencent/HY-Motion-1.0 \
    --prompt_engineering_host http://localhost:8000/v1 --port 7861

# Without prompt engineering:
python custom/web_server.py --model_path ckpts/tencent/HY-Motion-1.0 --port 7861
```

## API

- `POST /api/generate_json` — Generate motion from text
    - Input: unified motion request JSON. This server uses `text`, `duration`, `seed`,
      `cfg_scale`, `use_prompt_engineering`, `post_process`; other fields of the shared
      schema (`model`, `character`, `constraint_cfg`, `is_continuation`, `history`) are
      accepted but ignored.

        ```json
        {"text": "waving hello", "duration": 5.0, "seed": -1,
         "cfg_scale": 5.0, "use_prompt_engineering": true, "post_process": true}
        ```
    - Output: JSON with SMPLH params nested under `motion` (base64 float32 arrays + shapes,
      frame count, framerate, duration), plus the rewritten `prompt` and `_profile` timing.

        ```json
        {
          "motion": {
            "num_frames": 150, "framerate": 30, "duration": 5.0,
            "poses": "<base64 f32>", "poses_shape": [150, 156],
            "trans": "<base64 f32>", "trans_shape": [150, 3],
            "betas": "<base64 f32>", "betas_shape": [10]
          },
          "prompt": "...",
          "_profile": {}
        }
        ```
- `POST /api/generate_json_stream` — Same input, SSE streaming

## Files

- `web_server.py` — Gradio Web UI + REST API server
- `render_mp4_gpu.py` — GPU-accelerated NPZ-to-MP4 rendering (imported by web_server)

## Configuration

Service address is configured in `configs/settings/settings.json`:

```json
{
    "motion_generation": {
        "motion_api": "http://localhost:7861"
    }
}
```
