import base64
import io
import wave
from collections import deque

import numpy as np

from ..base.SpanProcessingStep import SpanProcessingStep

SAMPLE_RATE = 48000
RING_SECONDS = 60
STREAM_CHUNK_MS = 300


class VADStep(SpanProcessingStep):
    """Signal-driven voice segmentation with per-segment audio ownership.

    A permanent ring buffer holds the last ring_seconds of audio as rolling
    history, independent of turns; its only job is to supply the pre-roll
    (lookback) when a segment starts. Every incoming WAV chunk (audio_data)
    is appended to the ring — the pipeline MUST keep feeding audio after
    recording_end so the manual end tail can accumulate.

    Segmentation boundaries use the audio chunk-end timestamp axis and are
    converted to an exact sample inside the covering chunk:
      - recording_start: target timestamp = signal timestamp + signed
        manual_start_offset_ms. A negative value is lookback; a positive value
        points into future audio. A target in history is copied out of the
        ring; a future target waits for its covering audio chunk. A second
        start while active restarts the mark.
      - recording_end: target timestamp = signal timestamp + signed
        manual_end_offset_ms. A future target waits for tail audio; a target
        in captured audio trims at its exact sample. If end <= mark, the turn
        closes immediately with an empty WAV (or no audio chunk in stream
        mode).

    A model-driven subclass uses the exact same boundary machinery. Its start
    and end anchors are simply the timestamps of the latest chunks that
    produced the detector events, with its own start_offset_ms/end_offset_ms.

    A segment left open without an end boundary for ring_seconds is
    force-ended (warned) so the buffer stays bounded.

    Cancel (span semantics — current_timestamp is pinned to the mark for the
    whole segment, so a cancel is judged against the segment as a whole):
      - cancel stamp NEWER than the mark (mark < cancel): the segment is
        voided — mark and buffer dropped, nothing emitted, a later
        recording_end ignored.
      - cancel stamp NOT newer than the mark (cancel <= mark_ts): the segment
        is left untouched (it is newer than what the cancel invalidates) and
        keeps the audio it already captured.
      - either way the ring drops everything older than the cancel stamp, so
        a future segment's lookback can never reach cancelled audio.

    Output (single caught turn in, one or many audio_file outputs out):
      - stream=false: on finalize emit vad_end, then ONE audio_file WAV of
        [mark, end] carrying the start signal's pass_data flat.
      - stream=true: vad_start carries the start signal's pass_data
        (wrapped, like an SoS); from the mark onward, every full
        stream_chunk_ms of the buffer is emitted as an audio_file WAV chunk
        (timestamp only); with exact_chunk=true (default), the final short
        chunk is zero-padded to the same duration. exact_chunk=false keeps the
        natural short tail. Then vad_end. On cancel the envelope is NOT closed
        (the turn is stale).

    All outputs are stamped with the start signal's timestamp so cancel
    treats the whole segment as one turn.
    """

    REQUIRED_CATCH_SIGNALS = ["recording_start", "recording_end"]
    REQUIRED_INPUTS = ["audio_data"]
    OUTPUTS = ["audio_file"]
    EMIT_SIGNALS = ["vad_start", "vad_end"]

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        for key, default, minimum in (
                ("sample_rate", SAMPLE_RATE, 1),
                ("ring_seconds", RING_SECONDS, 1),
                ("stream_chunk_ms", STREAM_CHUNK_MS, 100)):
            v = config.get(key, default)
            if isinstance(v, bool) or not isinstance(v, (int, float)) \
                    or v < minimum:
                errors.append(f"{key} must be a number >= {minimum}, "
                              f"got {v!r}")
        for key in ("manual_start_offset_ms", "manual_end_offset_ms"):
            value = config.get(key, 0)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                errors.append(
                    f"{key} must be a signed number, got {value!r}")
        exact_chunk = config.get("exact_chunk", True)
        if not isinstance(exact_chunk, bool):
            errors.append(
                f"exact_chunk must be a bool, got {exact_chunk!r}")

        # A negative start offset is lookback and cannot exceed the ring.
        ring_s = config.get("ring_seconds", RING_SECONDS)
        start_ms = config.get("manual_start_offset_ms", 0)
        if not isinstance(start_ms, bool) and isinstance(start_ms, (int, float)) \
                and start_ms < 0 \
                and not isinstance(ring_s, bool) \
                and isinstance(ring_s, (int, float)) \
                and start_ms <= -ring_s * 1000:
            errors.append(
                f"manual_start_offset_ms ({start_ms}) must be > "
                f"-ring_seconds*1000 ({-ring_s * 1000}): the look-back "
                f"cannot exceed the ring")
        return errors

    def span_init(self):
        self.sample_rate = int(self.get_config("sample_rate", SAMPLE_RATE))
        self.ring_samples = int(
            self.get_config("ring_seconds", RING_SECONDS) * self.sample_rate)
        self.start_offset = int(self.get_config("manual_start_offset_ms", 0)
                                * self.sample_rate / 1000)
        self.end_offset = int(self.get_config("manual_end_offset_ms", 0)
                              * self.sample_rate / 1000)
        self.stream = self.get_config("stream", False)
        self.exact_chunk = self.get_config("exact_chunk", True)
        chunk_ms = int(self.get_config("stream_chunk_ms", STREAM_CHUNK_MS))
        chunk_ms = max(100, (chunk_ms // 100) * 100)  # multiples of 100ms
        self.chunk_samples = self.sample_rate * chunk_ms // 1000
        # a segment may run at most one ring's worth before it is force-ended
        self.seg_cap = self.ring_samples

        self._ring = deque()   # (start_sample, pcm_bytes, msg_ts) — history
        self._held = 0         # samples currently in the ring
        self._total = 0        # samples ever ingested
        self._rate_warned = False
        self._reset_turn()
        self.logger.info(
            f"vad ready: ring {self.ring_samples / self.sample_rate:.0f}s @ "
            f"{self.sample_rate}Hz, start/end offsets "
            f"{self.start_offset}/{self.end_offset} samples, "
            f"stream={self.stream} chunk={self.chunk_samples} samples"
        )

    def _reset_turn(self):
        self._mark = None        # segment start (absolute sample position)
        self._turn_ctx = None    # the start anchor's identity (stamp source)
        self._start_pass = {}
        self._start_forwarded = False
        self._end_target = None  # finalize once _total reaches this
        # Boundaries may point into a future audio chunk. Until that
        # chunk arrives the timestamp target, rather than _mark/_end_target,
        # represents the pending boundary.
        self._start_target_ts = None
        self._end_target_ts = None
        # Lightweight metadata for the active segment. Unlike the rolling
        # ring, this survives ring eviction, so a late end can still be
        # mapped to an exact sample without retaining a second PCM copy.
        self._timeline = []      # (absolute start_sample, samples, end_ts)
        self._seg = bytearray()  # segment audio from the mark (owned copy)
        self._emitted = 0        # stream: segment samples already emitted

    def on_span_cancel(self, cancel_message):
        """A cancel newer than the mark voids the whole segment: drop the
        mark and the captured buffer (nothing emitted; a later recording_end
        is ignored)."""
        self.logger.info("cancel - cleared vad mark and buffer")
        self._reset_turn()

    def _evict_cancelled(self):
        """Remove ring audio before the newest cancel timestamp.

        Chunk timestamps denote their ends, so a cancel can fall inside the
        first surviving chunk. Trim that chunk at the exact sample boundary;
        deleting whole chunks alone would let a later lookback recover the
        cancelled prefix. An active segment owns its PCM in _seg, so ring
        trimming never mutates live segment audio.
        """
        dropped = 0
        trimmed = 0
        epsilon = 0.5 / self.sample_rate
        while self._ring:
            start, pcm, end_ts = self._ring[0]
            samples = len(pcm) // 2
            chunk_start_ts = end_ts - samples / self.sample_rate

            if self.cancel_timestamp >= end_ts - epsilon:
                self._ring.popleft()
                self._held -= samples
                dropped += 1
                continue
            if self.cancel_timestamp <= chunk_start_ts + epsilon:
                break

            cut = round(
                (self.cancel_timestamp - chunk_start_ts) * self.sample_rate)
            cut = min(samples, max(0, cut))
            if cut:
                self._ring[0] = (start + cut, pcm[cut * 2:], end_ts)
                self._held -= cut
                trimmed += cut
            break
        if dropped or trimmed:
            self.logger.info(
                f"cancel - dropped {dropped} stale ring chunks, "
                f"trimmed {trimmed} samples"
            )

    def span_process(self, data, pass_data={}):
        self._evict_cancelled()
        signal = data.get("signal", "")
        if signal == "recording_start":
            self._on_start(data)
            return
        if signal == "recording_end":
            self._on_end(data)
            return

        audio_b64 = data.get("audio_data", "")
        if not audio_b64:
            return None
        chunk_start = self._total
        pcm = self._ingest(audio_b64, data.get("timestamp"))
        if pcm is None:
            return None

        # A start can only be mapped once the chunk covering its
        # timestamp exists. Resolving here snapshots the ring through this
        # chunk, so the normal append path must not append it a second time.
        resolved_start = False
        if self._mark is None and self._start_target_ts is not None:
            resolved_start = self._resolve_timestamp_start()

        if self._mark is not None:
            if not resolved_start:
                self._timeline.append(
                    (chunk_start, len(pcm) // 2, data.get("timestamp")))
                capture_start = max(self._mark, chunk_start)
                if self._total > capture_start:
                    self._seg += pcm[(capture_start - chunk_start) * 2:]
            if not self._start_forwarded and self._total >= self._mark:
                self._forward_start()

            if self._end_target is None and self._end_target_ts is not None:
                self._resolve_timestamp_end()

            # a segment left open past the cap (no recording_end) is
            # force-ended
            if self._start_forwarded and self._end_target is None \
                    and self._end_target_ts is None \
                    and (self._total - self._mark) >= self.seg_cap:
                self.logger.warning(
                    f"segment exceeded "
                    f"{self.seg_cap / self.sample_rate:.0f}s without "
                    f"recording_end; force-ending"
                )
                self._end_target = self._mark + self.seg_cap
            if self.stream and self._start_forwarded:
                self._drain_chunks()
            if self._end_target is not None \
                    and self._total >= max(self._mark, self._end_target):
                self._finalize()
        # the ingested (rate-normalized) PCM, for subclasses that consume
        # the same bytes the ring did (e.g. a detector feed)
        return pcm

    # ── signal handlers ──

    def _on_start(self, data):
        ts = data.get("timestamp")
        if self.span_active:
            self.logger.warning(
                "recording_start while active - discarding the open segment "
                "and rebuilding from the new mark")
        self._reset_turn()
        self._turn_ctx = self.stamp({}, data)  # internal retention: data view
        self._start_pass = dict(data.get("pass_data") or {})
        self.start_span(ts)

        self._start_target_ts = (
            ts + self.start_offset / self.sample_rate)
        if self._resolve_timestamp_start():
            return
        self.logger.info(
            f"vad start pending at timestamp {self._start_target_ts:.6f}"
        )

    def _forward_start(self):
        if self._start_forwarded:
            return
        self._start_forwarded = True
        start_msg = self.stamp({}, self._turn_ctx)
        if self.stream:
            self.envelope(start_msg, self._start_pass, wrap=True)
        self.emit_signal("vad_start", start_msg)
        if self.stream:
            self._drain_chunks()

    def _on_end(self, data):
        if not self.span_active or self._turn_ctx is None:
            self.logger.info("recording_end without active mark - ignored")
            return

        self._end_target = None
        self._end_target_ts = (
            data.get("timestamp") + self.end_offset / self.sample_rate)

        # Both boundaries are known and already form an empty interval. No
        # audio chunk is needed merely to prove that fact.
        if self._mark is None and self._start_target_ts is not None \
                and self._end_target_ts <= self._start_target_ts:
            self._mark = self._total
            self._start_target_ts = None
            self._end_target = self._mark
            self._end_target_ts = None
            self._finalize()
            return

        if not self._resolve_timestamp_end():
            self.logger.info(
                f"vad end pending at timestamp {self._end_target_ts:.6f}"
            )
            return

        if self._mark is not None and (
                self._end_target <= self._mark
                or self._total >= self._end_target):
            self._finalize()

    # ── ring (history) ──

    def _ingest(self, audio_b64, msg_ts):
        try:
            with wave.open(io.BytesIO(base64.b64decode(audio_b64)),
                           "rb") as wf:
                sr = wf.getframerate()
                pcm = wf.readframes(wf.getnframes())
        except Exception as e:
            self.logger.error(f"failed to decode audio chunk: {e}")
            return None
        if sr != self.sample_rate:
            # rate mismatch is only discoverable at runtime (first chunk),
            # so it degrades gracefully: every chunk is resampled to the
            # configured rate — margins, ring and the segment WAV header
            # all stay correct. One warning flags the conversion cost.
            if not self._rate_warned:
                self.logger.warning(
                    f"chunk sample rate {sr} != configured "
                    f"{self.sample_rate}; resampling every chunk — set the "
                    f"node's sample_rate to the client's rate to avoid it")
                self._rate_warned = True
            pcm = self._resample(pcm, sr)
        n = len(pcm) // 2
        if n == 0:
            return None  # empty frame carries no audio; keep it out of the ring
        self._ring.append((self._total, pcm, msg_ts))
        self._total += n
        self._held += n
        while self._held > self.ring_samples and len(self._ring) > 1:
            _, old, _ = self._ring.popleft()
            self._held -= len(old) // 2
        return pcm

    def _resample(self, pcm, src_rate):
        """Linear-interp PCM16 to the configured rate (speech-grade)."""
        x = np.frombuffer(pcm, dtype=np.int16)
        n_out = round(len(x) * self.sample_rate / src_rate)
        pos = np.arange(n_out) * (src_rate / self.sample_rate)
        return np.interp(pos, np.arange(len(x)),
                         x.astype(np.float64)).astype(np.int16).tobytes()

    def _slice(self, a, b):
        """PCM bytes for the absolute sample range [a, b), a >= ring start."""
        out = bytearray()
        for start, pcm, _ in self._ring:
            n = len(pcm) // 2
            if start + n <= a:
                continue
            if start >= b:
                break
            lo, hi = max(a, start), min(b, start + n)
            out += pcm[(lo - start) * 2:(hi - start) * 2]
        return bytes(out)

    # ── timestamp/sample mapping for all boundaries ──

    def _ring_timeline(self):
        return [
            (start, len(pcm) // 2, end_ts)
            for start, pcm, end_ts in self._ring
        ]

    def _timestamp_to_sample(self, target_ts, timeline):
        """Map a chunk-end timestamp to an absolute sample boundary.

        A timeline item ``(start, n, end_ts)`` covers the media interval
        ``[end_ts - n/rate, end_ts)``. ``round`` is intentional: epoch-sized
        float timestamps otherwise produce occasional one-sample truncation.
        Targets older than retained history clamp to its first sample; targets
        newer than the latest chunk stay unresolved until more audio arrives.
        """
        epsilon = 0.5 / self.sample_rate
        for start, samples, end_ts in timeline:
            if end_ts is None:
                continue
            if target_ts <= end_ts + epsilon:
                relative = samples - round(
                    (end_ts - target_ts) * self.sample_rate)
                relative = min(samples, max(0, relative))
                return start + relative
        return None

    def _resolve_timestamp_start(self):
        if self._start_target_ts is None:
            return False
        ring_timeline = self._ring_timeline()
        mark = self._timestamp_to_sample(
            self._start_target_ts, ring_timeline)
        if mark is None:
            return False

        self._mark = mark
        self._start_target_ts = None
        # Include the chunk ending exactly at the mark as useful boundary
        # metadata; _slice's half-open range still excludes its PCM.
        self._timeline = [
            item for item in ring_timeline
            if item[0] + item[1] >= self._mark
        ]
        self._seg = bytearray(self._slice(self._mark, self._total))
        self.logger.info(
            f"vad mark at sample {self._mark} from start anchor timestamp"
        )
        # recording_end may have arrived while this start target was still
        # waiting for audio. Resolve it before _forward_start drains stream
        # chunks, otherwise data beyond an already-known end could escape.
        if self._end_target_ts is not None:
            self._resolve_timestamp_end()
        self._forward_start()
        return True

    def _resolve_timestamp_end(self):
        if self._end_target_ts is None:
            return False
        timeline = self._timeline or self._ring_timeline()
        end_target = self._timestamp_to_sample(
            self._end_target_ts, timeline)
        if end_target is None:
            return False
        self._end_target = end_target
        self._end_target_ts = None
        self.logger.info(
            f"vad end at sample {self._end_target} from end anchor timestamp"
        )
        return True

    # ── output (from the segment buffer) ──

    def _drain_chunks(self):
        if not self._start_forwarded:
            return
        seg_samples = len(self._seg) // 2
        if self._end_target is None:
            # A negative end offset trims already-received audio. Keep that
            # much tail buffered so stream output never needs retraction.
            limit = max(0, seg_samples - max(0, -self.end_offset))
        else:
            limit = min(
                seg_samples, max(0, self._end_target - self._mark))
        while limit - self._emitted >= self.chunk_samples:
            lo = self._emitted * 2
            hi = (self._emitted + self.chunk_samples) * 2
            self._emit_chunk(self._seg[lo:hi])
            self._emitted += self.chunk_samples

    def _finalize(self):
        if not self._start_forwarded:
            self._forward_start()
        seg_len = max(0, self._end_target - self._mark)
        if self.stream:
            # The end target may resolve inside a tail held back for a negative
            # offset. Release every now-known full chunk first so the one
            # handled below is genuinely the final (sub-chunk) remainder.
            self._drain_chunks()
            # remaining un-emitted audio as one final chunk; exact mode pads
            # a short tail to the configured chunk duration
            if self._emitted < seg_len:
                pcm = bytes(self._seg[self._emitted * 2:seg_len * 2])
                short = self.chunk_samples - (seg_len - self._emitted)
                if self.exact_chunk and short > 0:
                    pcm += b"\x00\x00" * short
                self._emit_chunk(pcm)
            self.emit_signal("vad_end", self.stamp({}, self._turn_ctx))
        else:
            pcm = bytes(self._seg[:seg_len * 2])
            self.logger.info(
                f"vad segment: {seg_len / self.sample_rate:.2f}s")
            # signal first, then the audio — downstream learns the segment
            # ended before its payload arrives
            self.emit_signal("vad_end", self.stamp({}, self._turn_ctx))
            output_data = {}
            self.add_output(output_data, "audio_file", self._pcm_to_wav(pcm))
            segment_ctx = self.stamp(dict(self._start_pass), self._turn_ctx)
            self.output_to_queue(output_data, segment_ctx, log_level=0)
        self._reset_turn()
        self.end_span()

    def _emit_chunk(self, pcm):
        output_data = {}
        self.add_output(output_data, "audio_file",
                        self._pcm_to_wav(bytes(pcm)))
        self.output_to_queue(output_data, self._turn_ctx,
                             is_add_pass_data=False, log_level=0)

    def _pcm_to_wav(self, pcm):
        bio = io.BytesIO()
        with wave.open(bio, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(self.sample_rate)
            wf.writeframes(pcm)
        return base64.b64encode(bio.getvalue()).decode("ascii")
