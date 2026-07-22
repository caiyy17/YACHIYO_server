import base64
import io
import wave

from ..base.BaseProcessingStep import BaseProcessingStep


class PadStep(BaseProcessingStep):
    """Length-align the non-stream products riding in one message.

    Every declared input is a lane: a base64 WAV (audio) or a per-frame
    list whose first frame carries a "header" (motion / video / ...).
    Each lane's actual duration is self-described — WAV header for audio,
    first-frame header for frame lists — so no extra wiring is needed.

    A target duration is picked by `mode`:
      "longest"  — the longest lane (default)
      "shortest" — the shortest lane
      "anchor"   — the `anchor` input's duration
    and every lane is resized to it: audio extends with silence, frame
    lists extend by repeating the last frame (copies carry no header);
    cuts drop tail samples/frames. A frame list's header duration is
    rewritten to the new actual length.

    The selected target is also emitted as the separate ``duration``
    product.  It is the message's standard duration even when a lane opts
    out of cutting or extension and therefore keeps a different actual
    length.

    Per-lane opt-outs via `behavior`: {"<input>": {"cut": false}} keeps
    that lane un-truncated, {"extend": false} keeps it un-extended (both
    default true). Lanes with unreadable duration are left untouched and
    excluded from the target computation.

    A typical "audio follows the motion" setup: mode "anchor", anchor
    "motion", behavior {"audio": {"cut": false}} — the audio stretches to
    the motion's length but is never truncated.
    """

    REQUIRED_INPUTS = []
    FREE_INPUTS = True  # lanes are config-defined

    MODES = ("longest", "shortest", "anchor")

    @classmethod
    def module_outputs(cls, config):
        # Every lane passes through under its own name; duration is the
        # independently published alignment target selected for the message.
        lanes = [v.get("target") for v in config.get("input_vars", [])
                 if isinstance(v, dict)
                 and isinstance(v.get("target"), str)]
        return lanes + ["duration"]

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        targets = {v.get("target") for v in config.get("input_vars", [])}
        mode = config.get("mode", "longest")
        if mode not in cls.MODES:
            errors.append(f"mode must be one of {list(cls.MODES)}, "
                          f"got {mode!r}")
        if mode == "anchor" and config.get("anchor") not in targets:
            errors.append(
                f"mode 'anchor' needs an 'anchor' naming one input "
                f"({sorted(t for t in targets if t) or 'none'}), "
                f"got {config.get('anchor')!r}")
        behavior = config.get("behavior", {})
        if not isinstance(behavior, dict):
            errors.append(f"behavior must be an object, got {behavior!r}")
        else:
            for k, v in behavior.items():
                if k not in targets:
                    errors.append(f"behavior key '{k}' is not an input")
                elif (not isinstance(v, dict) or not set(v) <= {"cut", "extend"}
                        or any(not isinstance(x, bool) for x in v.values())):
                    errors.append(
                        f"behavior['{k}'] must be an object with boolean "
                        f'"cut"/"extend", got {v!r}')
        return errors

    def custom_init(self):
        self.mode = self.get_config("mode", "longest")
        self.anchor = self.get_config("anchor", None)
        self.behavior = self.get_config("behavior", {})
        self.lanes = [v["target"] for v in self.config.get("input_vars", [])]

    def process(self, data, pass_data={}):
        values = {k: data.get(k) for k in self.lanes if k in data}
        info = {k: self._lane_duration(v) for k, v in values.items()}
        known = {k: d for k, (kind, d) in info.items() if kind is not None}

        target = None
        if known:
            if self.mode == "anchor":
                target = known.get(self.anchor)
                if target is None:
                    self.logger.warning(
                        f"anchor '{self.anchor}' missing or unreadable; "
                        f"message passed through unchanged")
            elif self.mode == "shortest":
                target = min(known.values())
            else:
                target = max(known.values())

        output_data = {}
        changes = []
        for k, v in values.items():
            kind, dur = info[k]
            new = v
            if target is not None and kind is not None and dur != target:
                flags = self.behavior.get(k, {})
                allowed = flags.get("cut", True) if dur > target \
                    else flags.get("extend", True)
                if allowed:
                    new = self._resize(kind, v, target)
                    changes.append(f"{k} {dur:.2f}s->{target:.2f}s")
            self.add_output(output_data, k, new)
        if target is not None:
            self.add_output(output_data, "duration", target)
        if changes:
            self.logger.info(f"pad ({self.mode}): {', '.join(changes)}")
        self.output_to_queue(output_data, pass_data)

    # ── duration probes / resizing ──

    @staticmethod
    def _wav_params(wav_bytes):
        with wave.open(io.BytesIO(wav_bytes), "rb") as wf:
            return (wf.getnchannels(), wf.getsampwidth(), wf.getframerate(),
                    wf.getnframes(), wf.readframes(wf.getnframes()))

    def _lane_duration(self, value):
        """-> (kind, duration): kind "wav" / "frames" / None (unreadable)."""
        if isinstance(value, str) and value:
            try:
                _, _, rate, nframes, _ = self._wav_params(
                    base64.b64decode(value))
                return "wav", nframes / rate
            except Exception:
                return None, None
        if isinstance(value, list) and value and isinstance(value[0], dict):
            header = value[0].get("header") or {}
            framerate = header.get("framerate")
            if header.get("duration") is not None:
                return "frames", float(header["duration"])
            if framerate:
                return "frames", len(value) / float(framerate)
        return None, None

    def _resize(self, kind, value, target):
        if kind == "wav":
            return self._resize_wav(value, target)
        return self._resize_frames(value, target)

    @staticmethod
    def _resize_wav(b64, target):
        ch, width, rate, nframes, pcm = PadStep._wav_params(
            base64.b64decode(b64))
        want = int(round(target * rate))
        if want == nframes:
            return b64
        stride = ch * width
        pcm = pcm[:want * stride] if want < nframes \
            else pcm + b"\x00" * ((want - nframes) * stride)
        bio = io.BytesIO()
        with wave.open(bio, "wb") as wf:
            wf.setnchannels(ch)
            wf.setsampwidth(width)
            wf.setframerate(rate)
            wf.writeframes(pcm)
        return base64.b64encode(bio.getvalue()).decode("ascii")

    @staticmethod
    def _resize_frames(frames, target):
        header = dict(frames[0].get("header") or {})
        framerate = header.get("framerate")
        if not framerate:
            return frames  # cannot resize without a rate
        want = int(round(target * framerate))
        if want == len(frames):
            return frames
        if want <= 0:
            return []
        if want < len(frames):
            new = list(frames[:want])
        else:
            # repeated copies of the last frame carry no header
            filler = {k: v for k, v in frames[-1].items() if k != "header"}
            new = list(frames) + [dict(filler)
                                  for _ in range(want - len(frames))]
        header["duration"] = want / framerate
        first = dict(new[0])
        first["header"] = header
        new[0] = first
        return new
