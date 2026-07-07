import base64
import json

import numpy as np
import requests

from ..motion_base.MotionStep import MotionStep
from .smplh_to_humanoid import smplh_to_humanoid
from utils.settings import get_setting

# Continuous mode: after each request, the last `history_size` frames of the returned
# motion are kept and sent back as continuation context on the next request, so the
# backend can generate a smoothly connected follow-up. The returned motion itself is
# always passed downstream unchanged.
FRAMERATE = 30


def _b64_encode_f32(arr):
    return base64.b64encode(
        np.ascontiguousarray(arr, dtype=np.float32).tobytes()
    ).decode("ascii")


def _b64_decode_f32(b64_str, shape):
    return (
        np.frombuffer(base64.b64decode(b64_str), dtype=np.float32)
        .reshape(shape)
        .copy()
    )


class MotionGenerationCaller:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

        # Motion model config (configs/settings/motion.json, keyed by model name):
        # api_base + model_name + extra. extra (seed / use_prompt_engineering / post_process /
        # cfg_scale / constraint_cfg) is forwarded verbatim to the backend, so swapping models
        # = swapping the entry / its extra.
        model_name = self.config.get("model", "hy_motion")
        with open("configs/settings/motion.json") as f:
            self.model_config = json.load(f)[model_name]
        self.logger.info(f"Motion Model Config: {self.model_config}")
        api_base = self.model_config.get("api_base", "") or "motion_api"
        self.addr_motion = get_setting("motion_generation", api_base)
        self.model_name = self.model_config.get("model_name", model_name)
        self.extra = self.model_config.get("extra", {})

        # Top-level request semantics (from pipeline node config)
        self.duration = self.config.get("duration", 0)
        self.character = self.config.get("character", "")

        # When true, convert the SMPL-H result to the engine-native humanoid format before
        # returning (what HumanoidMotionPlayer consumes). Set false in config to keep raw SMPL-H.
        self.humanoid_output = bool(self.config.get("humanoid_output", True))

        self.continuous = bool(self.config.get("continuous", False))
        self.history_size = max(1, int(self.config.get("history_size", 5)))
        self.reset_history()

    def reset_history(self):
        self.history_poses = None
        self.history_trans = None
        self.history_betas = None

    def _has_history(self):
        return self.history_poses is not None

    def call(self, prompt):
        try:
            body = {
                "model": self.model_name,
                "text": prompt,
                "character": self.character,
                "duration": self.duration,
                "is_continuation": False,
            }
            # extra (seed / use_prompt_engineering / post_process / cfg_scale /
            # constraint_cfg) is forwarded verbatim from the motion model config.
            body.update(self.extra)

            if self.continuous and self._has_history():
                body["is_continuation"] = True
                body["history"] = {
                    "num_frames": int(self.history_poses.shape[0]),
                    "poses": _b64_encode_f32(self.history_poses),
                    "poses_shape": list(self.history_poses.shape),
                    "trans": _b64_encode_f32(self.history_trans),
                    "trans_shape": list(self.history_trans.shape),
                    "betas": _b64_encode_f32(self.history_betas),
                    "betas_shape": list(self.history_betas.shape),
                }

            response = requests.post(
                self.addr_motion + "/api/generate_json",
                json=body,
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()

            if self.continuous and isinstance(result, dict) and "error" not in result:
                self._save_history(result)

            if self.humanoid_output:
                return self._to_humanoid(result)
            # raw path: emit only the motion payload (num_frames/framerate/duration + SMPL-H)
            if isinstance(result, dict) and "motion" in result:
                return result["motion"]
            return result
        except Exception as e:
            self.logger.error(f"Failed to call motion generation: {e}")
            return ""

    def call_stream(self, prompt):
        """Stream motion chunks from the backend's SSE endpoint
        (/api/generate_json_stream), yielding one payload per delta.

        humanoid_output: deltas are converted incrementally — prev_trans /
        ref_y carry the cross-chunk state so the concatenation of chunk
        conversions equals one whole-clip conversion (no root-step loss or
        hips jump at chunk boundaries). Otherwise raw SMPL-H chunks are
        yielded in the non-stream motion schema. Continuation history is
        saved from the accumulated tail exactly like call().
        """
        try:
            body = {
                "model": self.model_name,
                "text": prompt,
                "character": self.character,
                "duration": self.duration,
                "is_continuation": False,
            }
            body.update(self.extra)
            if self.continuous and self._has_history():
                body["is_continuation"] = True
                body["history"] = {
                    "num_frames": int(self.history_poses.shape[0]),
                    "poses": _b64_encode_f32(self.history_poses),
                    "poses_shape": list(self.history_poses.shape),
                    "trans": _b64_encode_f32(self.history_trans),
                    "trans_shape": list(self.history_trans.shape),
                    "betas": _b64_encode_f32(self.history_betas),
                    "betas_shape": list(self.history_betas.shape),
                }
            stream_size = int(self.config.get("stream_size", 0))
            if stream_size > 0:
                body["stream_size"] = stream_size

            response = requests.post(
                self.addr_motion + "/api/generate_json_stream",
                json=body,
                stream=True,
                timeout=30,
            )
            response.raise_for_status()
            framerate = float(response.headers.get("X-Framerate", FRAMERATE))

            prev_trans = None   # last root translation of the previous chunk
            ref_y = None        # session first-frame pelvis Y
            tail_poses = tail_trans = None  # rolling tail for continuation
            betas = None

            for line in response.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data: "):
                    continue
                event = json.loads(line[len("data: "):])
                etype = event.get("type", "")

                if etype == "motion.delta":
                    if "poses" not in event:
                        # skeleton-only backend: forward the raw delta
                        yield event
                        continue
                    poses = _b64_decode_f32(event["poses"], event["poses_shape"])
                    trans = _b64_decode_f32(event["trans"], event["trans_shape"])
                    n = int(event["num_frames"])

                    if self.continuous:
                        if tail_poses is None:
                            tail_poses, tail_trans = poses, trans
                        else:
                            tail_poses = np.concatenate([tail_poses, poses])
                            tail_trans = np.concatenate([tail_trans, trans])
                        tail_poses = tail_poses[-self.history_size:]
                        tail_trans = tail_trans[-self.history_size:]

                    if self.humanoid_output:
                        h = smplh_to_humanoid(
                            poses, trans, n, framerate=framerate,
                            prev_trans=prev_trans, ref_y=ref_y,
                        )
                        if ref_y is None:
                            ref_y = float(trans[0][1])
                        prev_trans = [float(v) for v in trans[-1]]
                        yield h
                    else:
                        yield {
                            "num_frames": n,
                            "framerate": framerate,
                            "duration": n / framerate,
                            "format": "smplh",
                            "poses": event["poses"],
                            "poses_shape": event["poses_shape"],
                            "trans": event["trans"],
                            "trans_shape": event["trans_shape"],
                        }

                elif etype == "motion.done":
                    if "betas" in event:
                        betas = _b64_decode_f32(event["betas"], event["betas_shape"])

                elif etype == "error":
                    self.logger.error(f"motion stream error: {event.get('error')}")
                    break

            if self.continuous and tail_poses is not None:
                self.history_poses = tail_poses.copy()
                self.history_trans = tail_trans.copy()
                self.history_betas = (betas if betas is not None
                                      else np.zeros(10, dtype=np.float32))
        except Exception as e:
            self.logger.error(f"Failed to stream motion generation: {e}")

    def _save_history(self, result):
        """Keep the last `history_size` frames of the returned motion as continuation
        context for the next request. Does NOT modify the returned motion."""
        N = self.history_size
        m = result["motion"]
        self.history_poses = _b64_decode_f32(m["poses"], m["poses_shape"])[-N:].copy()
        self.history_trans = _b64_decode_f32(m["trans"], m["trans_shape"])[-N:].copy()
        self.history_betas = _b64_decode_f32(m["betas"], m["betas_shape"]).copy()

    def _to_humanoid(self, result):
        """Convert the returned SMPL-H motion (result["motion"]) to the humanoid format
        the Unity HumanoidMotionPlayer consumes. Continuation history is stored separately
        in SMPL-H space (see _save_history). Errors / non-dict pass through unchanged."""
        if not isinstance(result, dict) or "error" in result or "motion" not in result:
            return result
        m = result["motion"]
        ps = m.get("poses_shape")
        n = int(ps[0]) if ps else int(m.get("num_frames", 0))
        poses = _b64_decode_f32(m["poses"], m["poses_shape"])
        trans = _b64_decode_f32(m["trans"], m["trans_shape"])
        h = smplh_to_humanoid(poses, trans, n, framerate=m.get("framerate", FRAMERATE))
        if "duration" in m:
            h["duration"] = m["duration"]
        return h


class MotionGenerationStep(MotionStep):
    """Continuous mode needs config: catch_signals+pass_signals: ["SoS"]."""

    def custom_init(self):
        self.motion_caller = MotionGenerationCaller(self.config, self.logger)
