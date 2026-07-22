"""
HY-Motion Web Server — Web UI + REST API for text-to-motion generation.

Provides:
  - Web UI: text input, duration slider, seed, CFG scale → download SMPLH NPZ
  - Prompt rewriting + automatic duration estimation (via --prompt_engineering_host)
  - REST API:
      POST /api/generate_json  — returns JSON {motion: {SMPL-H params as base64 float
                                 arrays, num_frames, framerate, duration}, prompt, _profile}
      POST /api/generate       — (auto-exposed by Gradio, returns NPZ files)

Usage:
    # With prompt engineering (auto rewrite + duration estimation):
    python custom/web_server.py --model_path ckpts/tencent/HY-Motion-1.0 \
        --prompt_engineering_host http://your-llm-api:8000/v1 --port 7861

    # Without prompt engineering:
    python custom/web_server.py --model_path ckpts/tencent/HY-Motion-1.0 --port 7861
"""
import argparse
import base64
import os
import os.path as osp
import sys
import tempfile
import time

# Add project root to path so 'from hymotion...' works from custom/
PROJECT_ROOT = osp.dirname(osp.dirname(osp.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)

import numpy as np
import torch

import gradio as gr

from hymotion.utils.t2m_runtime import T2MRuntime
from hymotion.pipeline.body_model import construct_smpl_data_dict
from custom.render_mp4_gpu import render_npz_to_mp4

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
_runtime: T2MRuntime = None
_prompt_engineering_available: bool = False


def _init_runtime(args):
    global _runtime, _prompt_engineering_available
    cfg = osp.join(args.model_path, "config.yml")
    ckpt = osp.join(args.model_path, "latest.ckpt")

    if not osp.exists(cfg):
        raise FileNotFoundError(f"Config not found: {cfg}")

    skip_model_loading = not osp.exists(ckpt)
    if skip_model_loading:
        print(f"[WARNING] Checkpoint not found: {ckpt}, using random weights")

    # Determine if prompt engineering is available
    has_remote_host = bool(getattr(args, "prompt_engineering_host", None))
    has_local_prompter = osp.isdir("./ckpts/Text2MotionPrompter")
    disable_pe = getattr(args, "disable_prompt_engineering", False)
    _prompt_engineering_available = (has_remote_host or has_local_prompter) and not disable_pe

    if _prompt_engineering_available:
        print(f">>> Prompt engineering enabled (remote={has_remote_host}, local={has_local_prompter})")
    else:
        print(f">>> Prompt engineering disabled (remote={has_remote_host}, local={has_local_prompter}, disable={disable_pe})")

    device_ids = [int(d) for d in getattr(args, "device_ids", "3").split(",")]
    multi_gpu = getattr(args, "multi_gpu", False)

    _runtime = T2MRuntime(
        config_path=cfg,
        ckpt_name=ckpt,
        device_ids=device_ids,
        skip_model_loading=skip_model_loading,
        disable_prompt_engineering=not _prompt_engineering_available,
        prompt_engineering_host=getattr(args, "prompt_engineering_host", None),
        prompt_engineering_model_path=getattr(args, "prompt_engineering_model_path", None),
        multi_gpu=multi_gpu,
    )


# ---------------------------------------------------------------------------
# Warmup (trigger torch.compile before first request)
# ---------------------------------------------------------------------------
def _warmup_pipeline():
    """Warmup torch.compile CUDA graph for fixed sequence length (360 frames).
    All requests use the same seq_len, so only one compilation is needed.
    """
    print(">>> Warming up torch.compile CUDA graph (seq_len=360)...")
    t0 = time.time()
    pi = _runtime._acquire_pipeline()
    try:
        pipeline = _runtime.pipelines[pi]
        pipeline.eval()
        _ = pipeline.generate(
            text="warmup", seed_input=[0],
            duration_slider=1.0, cfg_scale=5.0,
            skip_decode=True,
        )
    finally:
        _runtime._release_pipeline(pi)
    print(f">>> Warmup complete in {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# Prompt rewrite + duration estimation
# ---------------------------------------------------------------------------
def rewrite_and_estimate(text: str):
    """Call LLM to rewrite prompt and estimate duration."""
    if not text.strip():
        return "", 5.0, "Please enter text first."
    try:
        duration, rewritten = _runtime.rewrite_text_and_infer_time(text)
        return rewritten, duration, f"Rewrite done. Estimated duration: {duration:.1f}s"
    except Exception as e:
        return text, 5.0, f"Rewrite failed: {e}. Using original text."


# ---------------------------------------------------------------------------
# Core generation function
# ---------------------------------------------------------------------------
def generate(
    text: str,
    rewritten_text: str,
    duration: float,
    seeds: str,
    cfg_scale: float,
    ground_align: bool,
):
    """Generate motion → save SMPLH + 201 NPZ + MP4 preview."""
    # Use rewritten text if prompt engineering is available and rewritten text is non-empty
    prompt = rewritten_text.strip() if _prompt_engineering_available and rewritten_text.strip() else text.strip()
    if not prompt:
        raise gr.Error("Text prompt cannot be empty.")

    seed_list = [int(s.strip()) for s in seeds.split(",") if s.strip()]
    if not seed_list:
        seed_list = [42]

    pi = _runtime._acquire_pipeline()
    try:
        pipeline = _runtime.pipelines[pi]
        pipeline.eval()
        model_output = pipeline.generate(
            text=prompt,
            seed_input=seed_list,
            duration_slider=duration,
            cfg_scale=cfg_scale,
            ground_align=ground_align,
        )
    finally:
        _runtime._release_pipeline(pi)

    rot6d = model_output["rot6d"]              # (B, T, J, 6)
    transl = model_output["transl"]            # (B, T, 3)
    latent_denorm = model_output["latent_denorm"]  # (B, T, 201)
    num_samples = rot6d.shape[0]

    tmp_dir = tempfile.mkdtemp(prefix="hymotion_")
    paths = []
    for b in range(num_samples):
        smpl_data = construct_smpl_data_dict(
            rot6d=rot6d[b].clone(),
            transl=transl[b].clone(),
        )
        ts = time.strftime("%Y%m%d_%H%M%S")
        seed_tag = seed_list[b] if b < len(seed_list) else b

        # Save SMPLH npz
        smplh_path = osp.join(tmp_dir, f"smplh_{ts}_seed{seed_tag}.npz")
        np.savez_compressed(
            smplh_path,
            poses=smpl_data["poses"],
            trans=smpl_data["trans"],
            betas=smpl_data["betas"],
            gender=np.array([smpl_data["gender"]]),
            Rh=smpl_data["Rh"],
            mocap_framerate=30,
            num_frames=smpl_data["num_frames"],
        )
        paths.append(smplh_path)

        # Save 201 npz
        motion201_path = osp.join(tmp_dir, f"motion201_{ts}_seed{seed_tag}.npz")
        ld = latent_denorm[b].numpy() if isinstance(latent_denorm[b], torch.Tensor) else latent_denorm[b]
        np.savez_compressed(
            motion201_path,
            motion=ld,
            mocap_framerate=30,
            num_frames=ld.shape[0],
        )
        paths.append(motion201_path)

    # Render first sample as MP4 preview (from SMPLH npz)
    mp4_path = render_npz_to_mp4(
        paths[0], title=prompt, fps=30, image_size=512
    )

    return paths, mp4_path


# ---------------------------------------------------------------------------
# JSON API (for Unity / non-Python clients)
# ---------------------------------------------------------------------------
def _encode_float_array(arr) -> str:
    """Encode a numpy array as base64 string (little-endian float32)."""
    arr = np.ascontiguousarray(arr, dtype=np.float32)
    return base64.b64encode(arr.tobytes()).decode("ascii")


def generate_json(
    text: str,
    duration: float = 5.0,
    seed: int = 42,
    cfg_scale: float = 5.0,
    use_prompt_engineering: bool = True,
    post_process: bool = True,
    ground_align: bool = False,
):
    """Generate motion and return SMPL params as a JSON-serializable dict.

    Args:
        post_process: If True, apply smoothing (slerp + savgol). If False,
            skip smoothing for faster output.
        ground_align: If True, run body model to compute ground offset and
            align feet to ground. Only takes effect when post_process=True.
    """
    _profile = {}
    prompt = text.strip()
    if not prompt:
        return {"error": "Text prompt cannot be empty."}

    # Prompt rewriting
    if use_prompt_engineering and _prompt_engineering_available:
        try:
            t0 = time.perf_counter()
            est_duration, rewritten = _runtime.rewrite_text_and_infer_time(prompt)
            _profile["prompt_rewrite"] = time.perf_counter() - t0
            prompt = rewritten
            if duration <= 0:
                duration = est_duration
        except Exception:
            pass  # fall back to original prompt

    skip_decode = not post_process
    t0 = time.perf_counter()
    pi = _runtime._acquire_pipeline()
    try:
        pipeline = _runtime.pipelines[pi]
        pipeline.eval()
        model_output = pipeline.generate(
            text=prompt,
            seed_input=[seed],
            duration_slider=duration,
            cfg_scale=cfg_scale,
            skip_decode=skip_decode,
            ground_align=ground_align and not skip_decode,
        )
    finally:
        _runtime._release_pipeline(pi)
    _profile["pipeline_generate"] = time.perf_counter() - t0

    # Merge internal profiling from generate()
    if "_profile" in model_output:
        _profile.update(model_output.pop("_profile"))

    t0 = time.perf_counter()
    if skip_decode:
        # Extract rot6d and transl directly from denormalized latent
        ld = model_output["latent_denorm"][0]  # (T, 201)
        num_frames = ld.shape[0]
        transl = ld[:, 0:3]
        root_rot6d = ld[:, 3:9].reshape(num_frames, 1, 6)
        body_rot6d = ld[:, 9:9 + 21 * 6].reshape(num_frames, 21, 6)
        rot6d = torch.cat([root_rot6d, body_rot6d], dim=1)
        smpl_data = construct_smpl_data_dict(rot6d=rot6d, transl=transl)
    else:
        # _decode_o6dp already applied smoothing + ground alignment (via body
        # model vertices min_y), so just convert to SMPL format directly.
        rot6d = model_output["rot6d"]      # (B, T, J, 6)
        transl = model_output["transl"]    # (B, T, 3)
        smpl_data = construct_smpl_data_dict(
            rot6d=rot6d[0].clone(),
            transl=transl[0].clone(),
        )
    _profile["smpl_conversion"] = time.perf_counter() - t0

    # Log profiling summary (only sum top-level timers, not sub-timers from pipeline)
    _top_level_keys = {"prompt_rewrite", "pipeline_generate", "smpl_conversion"}
    total = sum(v for k, v in _profile.items() if isinstance(v, float) and k in _top_level_keys)
    print(f"\n{'='*60}")
    print(f"[Profile] generate_json() total: {total:.3f}s")
    for k, v in _profile.items():
        if isinstance(v, float):
            print(f"  {k}: {v:.3f}s ({v/total*100:.1f}%)")
        else:
            print(f"  {k}: {v}")
    print(f"{'='*60}\n")

    num_frames = int(smpl_data["num_frames"])
    framerate = 30
    return {
        "motion": {
            "num_frames": num_frames,
            "framerate": framerate,
            "duration": num_frames / framerate,
            "poses": _encode_float_array(smpl_data["poses"]),
            "poses_shape": list(smpl_data["poses"].shape),
            "trans": _encode_float_array(smpl_data["trans"]),
            "trans_shape": list(smpl_data["trans"].shape),
            "betas": _encode_float_array(smpl_data["betas"]),
            "betas_shape": list(smpl_data["betas"].shape),
        },
        "prompt": prompt,
        "_profile": _profile,
    }


def _generate_json_stream_worker(text, duration, seed, cfg_scale, use_prompt_engineering, send_event):
    """Run the full generation pipeline, streaming frame chunks via SSE.

    Skips smoothing and ground alignment so frames can be sent as soon as
    the ODE solver finishes — each chunk is converted to SMPL and sent immediately.
    """
    _profile = {}
    prompt = text.strip()
    if not prompt:
        send_event("error", {"message": "Text prompt cannot be empty."})
        return

    # Prompt rewriting
    duration_final = duration
    if use_prompt_engineering and _prompt_engineering_available:
        send_event("status", {"stage": "prompt_rewrite", "message": "Rewriting prompt..."})
        try:
            t0 = time.perf_counter()
            est_duration, rewritten = _runtime.rewrite_text_and_infer_time(prompt)
            _profile["prompt_rewrite"] = time.perf_counter() - t0
            prompt = rewritten
            if duration <= 0:
                duration_final = est_duration
            send_event("prompt", {"rewritten": prompt, "duration": duration_final})
        except Exception:
            send_event("prompt", {"rewritten": prompt, "duration": duration_final})

    # Pipeline generation with skip_decode — returns raw denormalized latent
    def on_progress(stage, step, total):
        send_event("progress", {"stage": stage, "step": step, "total": total})

    send_event("status", {"stage": "generating", "message": "Running inference pipeline..."})
    t0 = time.perf_counter()
    pi = _runtime._acquire_pipeline()
    try:
        pipeline = _runtime.pipelines[pi]
        pipeline.eval()
        model_output = pipeline.generate(
            text=prompt,
            seed_input=[seed],
            duration_slider=duration_final,
            cfg_scale=cfg_scale,
            progress_callback=on_progress,
            skip_decode=True,
        )
    finally:
        _runtime._release_pipeline(pi)
    _profile["pipeline_generate"] = time.perf_counter() - t0

    if "_profile" in model_output:
        _profile.update(model_output.pop("_profile"))

    # Extract rot6d and transl from denormalized latent (no smoothing, no alignment)
    latent_denorm = model_output["latent_denorm"]  # (B, T, 201)
    ld = latent_denorm[0]  # (T, 201)
    num_frames = ld.shape[0]
    transl = ld[:, 0:3]  # (T, 3)
    root_rot6d = ld[:, 3:9].reshape(num_frames, 1, 6)
    body_rot6d = ld[:, 9:9 + 21 * 6].reshape(num_frames, 21, 6)
    rot6d = torch.cat([root_rot6d, body_rot6d], dim=1)  # (T, 22, 6)

    # Send header with metadata and betas (constant across frames)
    betas = np.zeros((1, 16), dtype=np.float32)
    send_event("result_header", {
        "prompt": prompt,
        "num_frames": num_frames,
        "framerate": 30,
        "poses_shape": [num_frames, 156],
        "trans_shape": [num_frames, 3],
        "betas": _encode_float_array(betas),
        "betas_shape": [1, 16],
    })

    # Stream frame chunks — convert rot6d to axis-angle SMPL format per chunk
    CHUNK_SIZE = 30  # 1 second of motion per chunk
    t0 = time.perf_counter()
    for start in range(0, num_frames, CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, num_frames)
        chunk_smpl = construct_smpl_data_dict(
            rot6d=rot6d[start:end].clone(),
            transl=transl[start:end].clone(),
        )
        send_event("result_chunk", {
            "start_frame": start,
            "num_frames": end - start,
            "poses": _encode_float_array(chunk_smpl["poses"]),
            "trans": _encode_float_array(chunk_smpl["trans"]),
        })
    _profile["stream_decode"] = time.perf_counter() - t0

    # Log profiling
    total = sum(v for v in _profile.values() if isinstance(v, float))
    print(f"\n{'='*60}")
    print(f"[Profile] generate_json_stream() total: {total:.3f}s")
    for k, v in _profile.items():
        if isinstance(v, float):
            print(f"  {k}: {v:.3f}s ({v/total*100:.1f}%)")
        else:
            print(f"  {k}: {v}")
    print(f"{'='*60}\n")

    send_event("profile", {"_profile": _profile})


def _build_fastapi_app():
    """Create a FastAPI app with custom REST endpoints, then mount Gradio onto it."""
    import asyncio
    import json as _json

    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse, StreamingResponse

    app = FastAPI()

    @app.post("/api/generate_json")
    async def api_generate_json(request: Request):
        # Unified motion request schema:
        #   model, text, character, duration, is_continuation, history{...},
        #   seed, use_prompt_engineering, post_process, cfg_scale, constraint_cfg
        # HY-Motion reads only the subset its pipeline supports and ignores the rest:
        #   - model / character / constraint_cfg : not read by this handler
        #   - is_continuation / history          : HY-Motion has no continuation; always generates fresh
        body = await request.json()
        try:
            result = generate_json(
                text=body.get("text", ""),
                duration=float(body.get("duration", 5.0)),
                seed=int(body.get("seed", 42)),
                cfg_scale=float(body.get("cfg_scale", 5.0)),
                use_prompt_engineering=bool(body.get("use_prompt_engineering", True)),
                post_process=bool(body.get("post_process", True)),
                ground_align=bool(body.get("ground_align", False)),
            )
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)
        if "error" in result:
            return JSONResponse(result, status_code=400)
        return JSONResponse(result)

    @app.post("/api/generate_json_stream")
    async def api_generate_json_stream(request: Request):
        """SSE streaming endpoint. Same params as /api/generate_json.

        Events:
          status   — {"stage": "...", "message": "..."}
          prompt   — {"rewritten": "...", "duration": float}
          progress — {"stage": "ode_step", "step": int, "total": int}
          result   — same schema as /api/generate_json response
          done     — {}
          error    — {"message": "..."}
        """
        body = await request.json()
        text = body.get("text", "")
        duration = float(body.get("duration", 5.0))
        seed = int(body.get("seed", 42))
        cfg_scale = float(body.get("cfg_scale", 5.0))
        use_prompt_engineering = bool(body.get("use_prompt_engineering", True))

        queue = asyncio.Queue()
        loop = asyncio.get_event_loop()

        def send_event(event_type, data):
            loop.call_soon_threadsafe(queue.put_nowait, (event_type, data))

        def run_in_thread():
            try:
                _generate_json_stream_worker(
                    text, duration, seed, cfg_scale,
                    use_prompt_engineering, send_event,
                )
            except Exception as e:
                send_event("error", {"message": str(e)})
            finally:
                send_event("done", {})

        task = loop.run_in_executor(None, run_in_thread)

        async def event_stream():
            while True:
                # per-hop bound: 30s for the NEXT event, no total limit —
                # a steadily producing generation never trips this
                try:
                    event_type, data = await asyncio.wait_for(
                        queue.get(), timeout=30)
                except asyncio.TimeoutError:
                    yield ("event: error\ndata: "
                           + _json.dumps({"error": "generation stalled: "
                                          "no event within 30s"}) + "\n\n")
                    break
                yield f"event: {event_type}\ndata: {_json.dumps(data)}\n\n"
                if event_type == "done":
                    break
            await task

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    return app


# ---------------------------------------------------------------------------
# Gradio UI
# ---------------------------------------------------------------------------
def build_app():
    with gr.Blocks(title="HY-Motion Web Server") as demo:
        gr.Markdown("# HY-Motion: Text → SMPLH Motion")

        with gr.Row():
            with gr.Column(scale=2):
                text_input = gr.Textbox(
                    label="Text Prompt",
                    placeholder="e.g. A person walks forward slowly.",
                    lines=3,
                )

                # Rewritten text (visible only when prompt engineering is available)
                rewritten_text = gr.Textbox(
                    label="Rewritten Text (editable)",
                    placeholder="Rewritten text will appear here after clicking Rewrite.",
                    interactive=True,
                    visible=_prompt_engineering_available,
                )

                duration_slider = gr.Slider(
                    minimum=0.5, maximum=12.0, value=5.0, step=0.1,
                    label="Duration (seconds)",
                )

                with gr.Row():
                    if _prompt_engineering_available:
                        rewrite_btn = gr.Button("Rewrite & Estimate Duration", variant="secondary")
                    generate_btn = gr.Button("Generate Motion", variant="primary")

                ground_align_cb = gr.Checkbox(
                    label="Ground Align (shift feet to y=0)",
                    value=False,
                )

                with gr.Accordion("Advanced Settings", open=False):
                    seed_input = gr.Textbox(
                        label="Seeds (comma separated)",
                        value="42",
                        placeholder="42  or  0,1,2,3",
                    )
                    cfg_slider = gr.Slider(
                        minimum=1.0, maximum=10.0, value=5.0, step=0.1,
                        label="CFG Scale",
                        info="Higher = more faithful to prompt",
                    )

                status = gr.Textbox(label="Status", interactive=False)

            with gr.Column(scale=2):
                output_video = gr.Video(label="Preview", autoplay=True)
            with gr.Column(scale=1):
                output_files = gr.File(
                    label="Download (SMPLH + 201)",
                    file_count="multiple",
                )

        # --- Events ---

        # Rewrite button (only exists when prompt engineering is available)
        if _prompt_engineering_available:
            rewrite_btn.click(
                fn=lambda: "Rewriting prompt & estimating duration...",
                outputs=[status],
            ).then(
                fn=rewrite_and_estimate,
                inputs=[text_input],
                outputs=[rewritten_text, duration_slider, status],
            )

        # Generate button
        generate_btn.click(
            fn=lambda: "Generating motion...",
            outputs=[status],
        ).then(
            fn=generate,
            inputs=[text_input, rewritten_text, duration_slider, seed_input, cfg_slider, ground_align_cb],
            outputs=[output_files, output_video],
        ).then(
            fn=lambda files: f"Done! Generated {len(files)} file(s).",
            inputs=[output_files],
            outputs=[status],
        )

    return demo


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="HY-Motion Web Server")
    parser.add_argument("--model_path", type=str, required=True,
                        help="Path to model directory (contains config.yml + latest.ckpt)")
    parser.add_argument("--port", type=int, default=7861)
    parser.add_argument("--host", type=str, default="0.0.0.0")
    parser.add_argument("--share", action="store_true",
                        help="Create a public Gradio share link")
    parser.add_argument("--prompt_engineering_host", type=str, default=None,
                        help="OpenAI-compatible API host for prompt rewriting (e.g. http://host:8000/v1)")
    parser.add_argument("--prompt_engineering_model_path", type=str, default=None,
                        help="Local path to prompter model (default: ckpts/Text2MotionPrompter)")
    parser.add_argument("--disable_prompt_engineering", action="store_true",
                        help="Disable prompt rewriting even if model/host is available")
    parser.add_argument("--device_ids", type=str, default="3",
                        help="Comma-separated GPU device IDs (default: 3)")
    parser.add_argument("--multi_gpu", action="store_true",
                        help="Enable multi-GPU mode: distribute models across GPUs. "
                             "Requires 2+ GPUs (e.g. --device_ids 0,1). "
                             "GPU0=prompter(bf16), GPU1=text_encoder+DiT(bf16+compile).")
    args = parser.parse_args()

    # Restrict all CUDA operations to the specified devices
    os.environ["CUDA_VISIBLE_DEVICES"] = args.device_ids
    # After CUDA_VISIBLE_DEVICES is set, visible GPUs are re-indexed from 0
    args.device_ids = ",".join(str(i) for i in range(len(args.device_ids.split(","))))

    # Enable tf32 matmul for faster fp32 operations
    torch.backends.cuda.matmul.allow_tf32 = True
    torch.backends.cudnn.allow_tf32 = True

    print(f"Initializing model (CUDA_VISIBLE_DEVICES={os.environ['CUDA_VISIBLE_DEVICES']})...")
    _init_runtime(args)

    # Warmup torch.compile by running a dummy generation
    _warmup_pipeline()

    print("Building Gradio app...")
    demo = build_app()

    # Create FastAPI app with custom API routes, then mount Gradio onto it
    fastapi_app = _build_fastapi_app()
    fastapi_app = gr.mount_gradio_app(fastapi_app, demo, path="/")

    print(f"Launching server on {args.host}:{args.port}")
    import uvicorn
    uvicorn.run(fastapi_app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
