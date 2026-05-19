import base64
import random

import numpy as np
import requests

from ..data_query_base.DataQueryStep import DataQueryStep
from utils.settings import get_setting

addr_motion = get_setting("motion_generation", "addr_motion")

# API protocol: continuation requests blend the last 5 history frames with the
# first 5 returned frames. history_size must be >= 5.
SEAM_FRAMES = 5
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
        self.duration = self.config.get("duration", 0)
        self.seed = self.config.get("seed", 42)
        self.use_prompt_engineering = self.config.get("use_prompt_engineering", False)
        self.post_process = self.config.get("post_process", True)
        self.character = self.config.get("character", "")

        self.continuous = bool(self.config.get("continuous", False))
        self.history_size = int(self.config.get("history_size", SEAM_FRAMES))
        if self.history_size < SEAM_FRAMES:
            self.logger.error(
                f"history_size must be >= {SEAM_FRAMES}, "
                f"got {self.history_size}, clamping"
            )
            self.history_size = SEAM_FRAMES
        self.reset_history()

    def reset_history(self):
        self.history_poses = None
        self.history_trans = None
        self.history_betas = None

    def _has_history(self):
        return self.history_poses is not None

    def call(self, prompt):
        seed = self.seed if self.seed >= 0 else random.randint(0, 2**32 - 1)
        try:
            body = {
                "text": prompt,
                "duration": self.duration,
                "seed": seed,
                "use_prompt_engineering": self.use_prompt_engineering,
                "post_process": self.post_process,
                "character": self.character,
            }
            if "cfg_scale" in self.config:
                body["cfg_scale"] = self.config["cfg_scale"]
            if "constraint_cfg" in self.config:
                body["constraint_cfg"] = self.config["constraint_cfg"]

            if self.continuous and self._has_history():
                n = int(self.history_poses.shape[0])
                body["is_continuation"] = True
                body["n"] = n
                body["history"] = {
                    "poses": _b64_encode_f32(self.history_poses),
                    "poses_shape": list(self.history_poses.shape),
                    "trans": _b64_encode_f32(self.history_trans),
                    "trans_shape": list(self.history_trans.shape),
                    "betas": _b64_encode_f32(self.history_betas),
                    "betas_shape": list(self.history_betas.shape),
                }

            response = requests.post(
                addr_motion + "/api/generate_json",
                json=body,
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()

            if self.continuous and "error" not in result:
                result = self._update_history_and_truncate(result)

            return result
        except Exception as e:
            self.logger.error(f"Failed to call motion generation: {e}")
            return ""

    def _update_history_and_truncate(self, result):
        """Splice returned frames onto held history, save last N as new history,
        and return the rest as the result."""
        N = self.history_size
        poses = _b64_decode_f32(result["poses"], result["poses_shape"])
        trans = _b64_decode_f32(result["trans"], result["trans_shape"])
        betas = _b64_decode_f32(result["betas"], result["betas_shape"])

        if self._has_history():
            # API's first SEAM_FRAMES replace the last SEAM_FRAMES of held history.
            full_poses = np.concatenate(
                [self.history_poses[: N - SEAM_FRAMES], poses], axis=0
            )
            full_trans = np.concatenate(
                [self.history_trans[: N - SEAM_FRAMES], trans], axis=0
            )
        else:
            full_poses = poses
            full_trans = trans

        total = full_poses.shape[0]
        if total <= N:
            new_history_poses = full_poses.copy()
            new_history_trans = full_trans.copy()
            out_poses = full_poses[:0]
            out_trans = full_trans[:0]
        else:
            new_history_poses = full_poses[-N:].copy()
            new_history_trans = full_trans[-N:].copy()
            out_poses = full_poses[:-N]
            out_trans = full_trans[:-N]

        self.history_poses = new_history_poses
        self.history_trans = new_history_trans
        self.history_betas = betas

        result["poses"] = _b64_encode_f32(out_poses)
        result["poses_shape"] = list(out_poses.shape)
        result["trans"] = _b64_encode_f32(out_trans)
        result["trans_shape"] = list(out_trans.shape)
        result["num_frames"] = int(out_poses.shape[0])
        return result

    def flush(self):
        """Emit the held-back frames as a final segment, then clear history.
        Returns None if there is nothing to flush."""
        if not self.continuous or not self._has_history():
            return None
        n = int(self.history_poses.shape[0])
        result = {
            "poses": _b64_encode_f32(self.history_poses),
            "poses_shape": list(self.history_poses.shape),
            "trans": _b64_encode_f32(self.history_trans),
            "trans_shape": list(self.history_trans.shape),
            "betas": _b64_encode_f32(self.history_betas),
            "betas_shape": list(self.history_betas.shape),
            "num_frames": n,
            "framerate": FRAMERATE,
            "is_continuation": False,
            "prompt": "",
            "duration_s": n / float(FRAMERATE),
        }
        self.reset_history()
        return result


class MotionGenerationStep(DataQueryStep):
    def custom_init(self):
        self.data_query_caller = MotionGenerationCaller(self.config, self.logger)
        if self.data_query_caller.continuous:
            self.catch_signal_set = {"SoS", "EoS"}

    def custom_cancel(self, cancel_message):
        self.data_query_caller.reset_history()

    def process(self, data, pass_data={}):
        signal = data.get("signal", "")
        if signal == "SoS":
            self.data_query_caller.reset_history()
            self.output_to_queue({"signal": "SoS"}, pass_data)
            return
        if signal == "EoS":
            flushed = self.data_query_caller.flush()
            if flushed is not None:
                output_data = {}
                self.add_output(output_data, "result", flushed)
                self.output_to_queue(output_data, pass_data)
            self.output_to_queue({"signal": "EoS"}, pass_data)
            return
        super().process(data, pass_data)
