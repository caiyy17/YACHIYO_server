import base64
import io
import wave
from collections import deque

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
    recording_end so end_offset_ms of tail can accumulate.

    Segmentation is driven by caught signals (a real-VAD subclass swaps the
    signal source for model decisions; nothing else changes):
      - recording_start: mark = now - start_offset_ms (clamped to the ring
        start), emit vad_start, and SNAPSHOT [mark, now) out of the ring into
        the segment's own buffer. From then on the segment owns its audio:
        every later chunk is appended to that buffer, decoupled from the ring.
        A second start while active restarts the mark.
      - recording_end: finalize once end_offset_ms of tail has accumulated.
      - a segment left open (no recording_end) for ring_seconds is
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
      - stream=true: from the mark onward, every full stream_chunk_ms of the
        buffer is emitted as an audio_file WAV chunk (timestamp only); the
        final short chunk is zero-padded; then vad_end. On cancel the
        envelope is NOT closed (the turn is stale).

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
                ("start_offset_ms", 0, 0),
                ("end_offset_ms", 0, 0),
                ("stream_chunk_ms", STREAM_CHUNK_MS, 100)):
            v = config.get(key, default)
            if isinstance(v, bool) or not isinstance(v, (int, float)) \
                    or v < minimum:
                errors.append(f"{key} must be a number >= {minimum}, "
                              f"got {v!r}")
        # the lookback cannot exceed the ring, or a segment starts already at
        # the force-end cap and dies on its first chunk
        ring_s = config.get("ring_seconds", RING_SECONDS)
        start_ms = config.get("start_offset_ms", 0)
        if not isinstance(ring_s, bool) and isinstance(ring_s, (int, float)) \
                and not isinstance(start_ms, bool) \
                and isinstance(start_ms, (int, float)) \
                and start_ms >= ring_s * 1000:
            errors.append(
                f"start_offset_ms ({start_ms}) must be < ring_seconds*1000 "
                f"({ring_s * 1000}): the lookback cannot exceed the ring")
        return errors

    def span_init(self):
        self.sample_rate = int(self.get_config("sample_rate", SAMPLE_RATE))
        self.ring_samples = int(
            self.get_config("ring_seconds", RING_SECONDS) * self.sample_rate)
        self.start_offset = int(self.get_config("start_offset_ms", 0)
                                * self.sample_rate / 1000)
        self.end_offset = int(self.get_config("end_offset_ms", 0)
                              * self.sample_rate / 1000)
        self.stream = self.get_config("stream", False)
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
        self._mark_ts = None
        self._start_pass = {}
        self._end_target = None  # finalize once _total reaches this
        self._seg = bytearray()  # segment audio from the mark (owned copy)
        self._emitted = 0        # stream: segment samples already emitted

    def on_span_cancel(self, cancel_message):
        """A cancel newer than the mark voids the whole segment: drop the
        mark and the captured buffer (nothing emitted; a later recording_end
        is ignored)."""
        self.logger.info("cancel - cleared vad mark and buffer")
        self._reset_turn()

    def _evict_cancelled(self):
        """The ring is rolling history for future lookback; drop the prefix
        older than the newest cancel stamp so a later segment never looks
        back into cancelled audio. An active segment keeps its own copy in
        _seg, so this never touches live segment audio."""
        dropped = 0
        while self._ring and self._ring[0][2] < self.cancel_timestamp:
            _, old, _ = self._ring.popleft()
            self._held -= len(old) // 2
            dropped += 1
        if dropped:
            self.logger.info(f"cancel - dropped {dropped} stale ring chunks")

    def span_process(self, data, pass_data={}):
        self._evict_cancelled()
        signal = data.get("signal", "")
        if signal == "recording_start":
            self._on_start(data)
            return
        if signal == "recording_end":
            self._on_end()
            return

        audio_b64 = data.get("audio_data", "")
        if not audio_b64:
            return
        pcm = self._ingest(audio_b64, data.get("timestamp"))
        if pcm is None or self._mark is None:
            return
        self._seg += pcm
        # a segment left open past the cap (no recording_end) is force-ended
        if self._end_target is None \
                and (self._total - self._mark) >= self.seg_cap:
            self.logger.warning(
                f"segment exceeded {self.seg_cap / self.sample_rate:.0f}s "
                f"without recording_end; force-ending"
            )
            self._end_target = self._mark + self.seg_cap
        if self.stream:
            self._drain_chunks()
        if self._end_target is not None and self._total >= self._end_target:
            self._finalize()

    # ── signal handlers ──

    def _on_start(self, data):
        ts = data.get("timestamp")
        if self._mark is not None:
            self.logger.warning(
                "recording_start while active - discarding the open segment "
                "and rebuilding from the new mark")
        self._reset_turn()
        avail_start = self._total - self._held
        self._mark = max(self._total - self.start_offset, avail_start)
        self._mark_ts = ts
        self._start_pass = dict(data.get("pass_data") or {})
        self.start_span(ts)
        # snapshot the lookback out of the ring; the segment owns it from now
        self._seg = bytearray(self._slice(self._mark, self._total))
        self.logger.info(
            f"vad mark at sample {self._mark} "
            f"(lookback {(self._total - self._mark) / self.sample_rate:.2f}s)"
        )
        start_msg = {"timestamp": ts}
        if self.stream and self._start_pass:
            start_msg["pass_data"] = self._start_pass
        self.emit_signal("vad_start", start_msg)
        if self.stream:
            self._drain_chunks()

    def _on_end(self):
        if self._mark is None:
            self.logger.info("recording_end without active mark - ignored")
            return
        self._end_target = self._total + self.end_offset
        if self._total >= self._end_target:
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
        if sr != self.sample_rate and not self._rate_warned:
            self.logger.warning(
                f"chunk sample rate {sr} != configured {self.sample_rate}; "
                f"segment timing will be wrong")
            self._rate_warned = True
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

    # ── output (from the segment buffer) ──

    def _drain_chunks(self):
        seg_samples = len(self._seg) // 2
        limit = seg_samples if self._end_target is None \
            else min(seg_samples, self._end_target - self._mark)
        while limit - self._emitted >= self.chunk_samples:
            lo = self._emitted * 2
            hi = (self._emitted + self.chunk_samples) * 2
            self._emit_chunk(self._seg[lo:hi])
            self._emitted += self.chunk_samples

    def _finalize(self):
        seg_len = self._end_target - self._mark   # samples to emit
        if self.stream:
            # remaining un-emitted audio as one final chunk, zero-padded
            if self._emitted < seg_len:
                pcm = bytes(self._seg[self._emitted * 2:seg_len * 2])
                short = self.chunk_samples - (seg_len - self._emitted)
                if short > 0:
                    pcm += b"\x00\x00" * short
                self._emit_chunk(pcm)
            self.emit_signal("vad_end", {"timestamp": self._mark_ts})
        else:
            pcm = bytes(self._seg[:seg_len * 2])
            self.logger.info(
                f"vad segment: {seg_len / self.sample_rate:.2f}s")
            # signal first, then the audio — downstream learns the segment
            # ended before its payload arrives
            self.emit_signal("vad_end", {"timestamp": self._mark_ts})
            output_data = {}
            self.add_output(output_data, "audio_file", self._pcm_to_wav(pcm))
            segment_pass = dict(self._start_pass)
            segment_pass["timestamp"] = self._mark_ts
            self.output_to_queue(output_data, segment_pass, log_level=0)
        self._reset_turn()
        self.end_span()

    def _emit_chunk(self, pcm):
        output_data = {}
        self.add_output(output_data, "audio_file",
                        self._pcm_to_wav(bytes(pcm)))
        self.output_to_queue(output_data, {"timestamp": self._mark_ts},
                             is_add_pass_data=False, log_level=0)

    def _pcm_to_wav(self, pcm):
        bio = io.BytesIO()
        with wave.open(bio, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(self.sample_rate)
            wf.writeframes(pcm)
        return base64.b64encode(bio.getvalue()).decode("ascii")
