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


def _frames_with_info(frames, framerate, fmt, duration=None):
    """Attach clip-level info (framerate/format) to the FIRST frame of a
    per-frame list, wrapped under a single "header" key — frame payload
    keys stay uniform across all frames. Same contract as the streaming
    path. duration is only known for a whole clip (non-stream); streaming
    leaves it out."""
    if frames:
        header = {"framerate": framerate, "format": fmt}
        if duration is not None:
            header["duration"] = duration
        frames[0] = {"header": header, **frames[0]}
    return frames


def _humanoid_to_frames(h):
    """Transpose a struct-of-arrays humanoid clip (per-key per-frame arrays)
    into a list of self-contained per-frame structs, so a frame-level
    consumer (e.g. the webrtc splitter's data lane) can distribute them one
    per slot without knowing the motion schema. framerate is stream-level
    (carried once on the envelope), so it is dropped from each frame."""
    n = int(h.get("num_frames", 0))
    joints = h.get("joints", {})
    return [
        {
            "root_dxz": h["root_dxz"][f],
            "root_dy": h["root_dy"][f],
            "root_dyaw": h["root_dyaw"][f],
            "hips_pos": h["hips_pos"][f],
            "joints": {b: joints[b][f] for b in joints},
        }
        for f in range(n)
    ]


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

    def _request_duration(self, duration):
        """Effective reference duration for the backend: the per-message
        input wins, then the config value; falsy/None -> not sent at all
        (the backend picks its own length)."""
        d = self.duration if duration is None else duration
        return float(d) if d else None

    def call(self, prompt, duration=None):
        try:
            body = {
                "model": self.model_name,
                "text": prompt,
                "character": self.character,
                "is_continuation": False,
            }
            # extra (seed / use_prompt_engineering / post_process / cfg_scale /
            # constraint_cfg) is forwarded verbatim from the motion model
            # config. Applied BEFORE the reference duration so the dynamic
            # value (input / node config) overrides a settings-side one.
            body.update(self.extra)
            ref = self._request_duration(duration)
            if ref is not None:
                body["duration"] = ref

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

            return self._package(result)
        except Exception as e:
            self.logger.error(f"Failed to call motion generation: {e}")
            return ""

    def _package(self, result):
        """Backend JSON -> per-frame list, same format as the streaming path
        (self-contained frame structs; the first frame carries framerate,
        format and duration). Errors / unexpected shapes pass through
        unchanged."""
        if self.humanoid_output:
            h = self._to_humanoid(result)
            if not (isinstance(h, dict) and "joints" in h):
                return h  # error / passthrough
            frames = _humanoid_to_frames(h)
            fr = h.get("framerate", FRAMERATE)
            return _frames_with_info(frames, fr, "humanoid",
                                     h.get("duration", len(frames) / fr))
        # raw path: per-frame SMPL-H structs (same contract as streaming)
        if isinstance(result, dict) and "motion" in result:
            m = result["motion"]
            poses = _b64_decode_f32(m["poses"], m["poses_shape"])
            trans = _b64_decode_f32(m["trans"], m["trans_shape"])
            frames = [{"poses": poses[f].tolist(), "trans": trans[f].tolist()}
                      for f in range(int(poses.shape[0]))]
            fr = m.get("framerate", FRAMERATE)
            return _frames_with_info(frames, fr, "smplh",
                                     m.get("duration", len(frames) / fr))
        return result

    def call_stream(self, prompt, duration=None):
        """Stream motion chunks from the backend's SSE endpoint
        (/api/generate_json_stream).

        stream_frames > 0: client-side EXACT re-chunking (the counterpart of
        the TTS caller's _rechunk) — server deltas of arbitrary size (the
        backend rounds its flushes to the model's commit size) are buffered
        at the SMPL-H frame level and re-cut into blocks of exactly
        `stream_frames` frames. With exact_chunk=true (default), a clean final
        short block repeats its last frame to reach stream_frames; false keeps
        it short. The backend request duration is never changed.
        stream_frames == 0 (default): one payload per server delta,
        unchanged behavior.

        humanoid_output: blocks are converted incrementally — prev_trans /
        ref_y carry the cross-block state so the concatenation of block
        conversions equals one whole-clip conversion (no root-step loss or
        hips jump at block boundaries). Otherwise per-frame raw SMPL-H
        structs are yielded (same shape as the non-stream raw path).
        Continuation history is saved from the accumulated tail exactly
        like call().
        """
        try:
            body = {
                "model": self.model_name,
                "text": prompt,
                "character": self.character,
                "is_continuation": False,
            }
            # settings extra first: the dynamic reference duration wins
            body.update(self.extra)
            ref = self._request_duration(duration)
            if ref is not None:
                body["duration"] = ref
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
            stream_frames = int(self.config.get("stream_frames", 0))
            response = requests.post(
                self.addr_motion + "/api/generate_json_stream",
                json=body,
                stream=True,
                timeout=10,
            )
            response.raise_for_status()
            framerate = float(response.headers.get("X-Framerate", FRAMERATE))

            state = {"prev_trans": None,  # last root translation, prev block
                     "ref_y": None}       # session first-frame pelvis Y
            tail_poses = tail_trans = None  # rolling tail for continuation
            betas = None
            buf_poses = buf_trans = None    # exact re-chunk buffer

            # stream-level info (constant across the whole stream) — wrapped
            # under "header" on the VERY FIRST frame only, so a consumer can
            # read it once without a separate envelope. Subsequent frames
            # carry only their per-frame data.
            first_frame = [True]
            stream_info = {"framerate": framerate,
                           "format": "humanoid" if self.humanoid_output else "smplh"}

            def _make_block(poses, trans):
                """Convert/package one output block into a list of per-frame
                structs (frame-level, schema-agnostic — the counterpart of an
                audio chunk's frames), advancing the incremental-conversion
                state at ITS boundary. The stream's first frame additionally
                carries stream_info (framerate/format)."""
                n = int(poses.shape[0])
                if self.humanoid_output:
                    h = smplh_to_humanoid(
                        poses, trans, n, framerate=framerate,
                        prev_trans=state["prev_trans"], ref_y=state["ref_y"],
                    )
                    if state["ref_y"] is None:
                        state["ref_y"] = float(trans[0][1])
                    state["prev_trans"] = [float(v) for v in trans[-1]]
                    frames = _humanoid_to_frames(h)
                else:
                    # raw SMPL-H: one frame per element (same per-frame-list
                    # contract as humanoid, so the data lane distributes it
                    # the same way)
                    frames = [{"poses": poses[f].tolist(), "trans": trans[f].tolist()}
                              for f in range(n)]
                if first_frame[0] and frames:
                    frames[0] = {"header": dict(stream_info), **frames[0]}
                    first_frame[0] = False
                return frames

            for line in response.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data: "):
                    continue
                event = json.loads(line[len("data: "):])
                etype = event.get("type", "")

                if etype == "motion.delta":
                    poses = _b64_decode_f32(event["poses"], event["poses_shape"])
                    trans = _b64_decode_f32(event["trans"], event["trans_shape"])

                    if self.continuous:
                        if tail_poses is None:
                            tail_poses, tail_trans = poses, trans
                        else:
                            tail_poses = np.concatenate([tail_poses, poses])
                            tail_trans = np.concatenate([tail_trans, trans])
                        tail_poses = tail_poses[-self.history_size:]
                        tail_trans = tail_trans[-self.history_size:]

                    if stream_frames <= 0:
                        yield {"motion": _make_block(poses, trans)}
                        continue
                    # exact re-chunk: buffer frames, cut full blocks
                    if buf_poses is None:
                        buf_poses, buf_trans = poses, trans
                    else:
                        buf_poses = np.concatenate([buf_poses, poses])
                        buf_trans = np.concatenate([buf_trans, trans])
                    while buf_poses.shape[0] >= stream_frames:
                        yield {"motion": _make_block(buf_poses[:stream_frames],
                                                     buf_trans[:stream_frames])}
                        buf_poses = buf_poses[stream_frames:]
                        buf_trans = buf_trans[stream_frames:]

                elif etype == "motion.done":
                    if "betas" in event:
                        betas = _b64_decode_f32(event["betas"], event["betas_shape"])

                elif etype == "error":
                    self.logger.error(f"motion stream error: {event.get('error')}")
                    buf_poses = buf_trans = None  # drop the partial tail
                    break

            # final short block (stream ended cleanly with a remainder)
            if buf_poses is not None and buf_poses.shape[0] > 0:
                frames = _make_block(buf_poses, buf_trans)
                if self.config.get("exact_chunk", True) \
                        and stream_frames > 0 \
                        and frames \
                        and len(frames) < stream_frames:
                    filler = {
                        key: value for key, value in frames[-1].items()
                        if key != "header"
                    }
                    frames.extend(
                        dict(filler) for _ in range(stream_frames - len(frames))
                    )
                yield {"motion": frames}

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
