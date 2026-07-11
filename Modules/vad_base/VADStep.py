import base64
import io
import wave
from collections import deque

from ..base.SpanProcessingStep import SpanProcessingStep

SAMPLE_RATE = 48000
RING_SECONDS = 60
STREAM_CHUNK_MS = 300


class VADStep(SpanProcessingStep):
    """Signal-driven voice segmentation over a persistent ring of audio.

    Consumes a continuous stream of WAV chunks (audio_data, e.g. one 100ms
    chunk per group from frame_collector — the pipeline MUST keep feeding
    audio after recording_end for end_offset_ms to elapse). Every chunk is
    written into a ring buffer of sample_rate * ring_seconds samples; the
    ring is permanent history, independent of turns.

    Segmentation is driven by caught signals (a real-VAD subclass replaces
    the signal source with model decisions; everything else stays):
      - recording_start: mark = current position - start_offset_ms, emit
        vad_start (in stream mode it carries the start signal's pass_data,
        like SoS). A second start while active restarts the mark.
      - recording_end: finalize once the ring has filled to end position +
        end_offset_ms.
      - cancel: clears the mark via the span machinery — nothing is emitted,
        a later recording_end is ignored. Ring content is untouched.

    Output (single-in-multi-out, same protocol as stream TTS):
      - stream=false: on finalize emit vad_end, then ONE audio_file WAV of
        [mark, end+end_offset] carrying the start signal's pass_data flat.
      - stream=true: from the mark onward, every time a full stream_chunk_ms
        of audio is available, emit an audio_file WAV chunk (timestamp
        only); the final short chunk is zero-padded to full size; then emit
        vad_end. On cancel the envelope is NOT closed (turn is stale).

    A recording longer than the ring keeps updating the ring (warned once
    the moment it overflows); the finalized audio is then just the last
    ring_seconds worth. All outputs are stamped with the start signal's
    timestamp so cancel treats the whole segment as one turn.
    """

    REQUIRED_CATCH_SIGNALS = ["recording_start", "recording_end"]
    REQUIRED_INPUTS = ["audio_data"]
    EMIT_SIGNALS = ["vad_start", "vad_end"]
    LOG_CONTENT = False  # 10 chunks/s of b64 WAV — signals still log

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

        self._ring = deque()   # (start_sample, pcm_bytes)
        self._held = 0         # samples currently held
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
        self._next_emit = None   # stream: next un-emitted sample
        self._overflow_warned = False

    def on_span_cancel(self, cancel_message):
        self.logger.info("cancel - cleared vad mark")
        self._reset_turn()

    def span_process(self, data, pass_data={}):
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
        self._ingest(audio_b64)
        if not self.span_active:
            return
        if self.stream:
            self._drain_chunks()
        if self._end_target is not None and self._total >= self._end_target:
            self._finalize()

    # ── signal handlers ──

    def _on_start(self, data):
        ts = data.get("timestamp")
        if self.span_active:
            self.logger.info("recording_start while active - restarting mark")
        self._reset_turn()
        avail_start = self._total - self._held
        self._mark = max(self._total - self.start_offset, avail_start)
        self._mark_ts = ts
        self._start_pass = dict(data.get("pass_data") or {})
        self.start_span(ts)
        self.logger.info(
            f"vad mark at sample {self._mark} "
            f"(lookback {(self._total - self._mark) / self.sample_rate:.2f}s)"
        )
        start_msg = {"timestamp": ts}
        if self.stream and self._start_pass:
            start_msg["pass_data"] = self._start_pass
        self.emit_signal("vad_start", start_msg)
        if self.stream:
            self._next_emit = self._mark
            self._drain_chunks()

    def _on_end(self):
        if not self.span_active:
            self.logger.info("recording_end without active mark - ignored")
            return
        self._end_target = self._total + self.end_offset
        if self._total >= self._end_target:
            self._finalize()

    # ── ring ──

    def _ingest(self, audio_b64):
        try:
            with wave.open(io.BytesIO(base64.b64decode(audio_b64)),
                           "rb") as wf:
                sr = wf.getframerate()
                pcm = wf.readframes(wf.getnframes())
        except Exception as e:
            self.logger.error(f"failed to decode audio chunk: {e}")
            return
        if sr != self.sample_rate and not self._rate_warned:
            self.logger.warning(
                f"chunk sample rate {sr} != configured {self.sample_rate}; "
                f"segment timing will be wrong")
            self._rate_warned = True
        n = len(pcm) // 2
        self._ring.append((self._total, pcm))
        self._total += n
        self._held += n
        while self._held > self.ring_samples and len(self._ring) > 1:
            _, old = self._ring.popleft()
            self._held -= len(old) // 2
        if (self.span_active and not self._overflow_warned
                and self._mark is not None
                and self._mark < self._total - self._held):
            self.logger.warning(
                "recording exceeds ring capacity; only the last "
                f"{self.ring_samples / self.sample_rate:.0f}s will be sent"
            )
            self._overflow_warned = True

    def _slice(self, a, b):
        """PCM bytes for the absolute sample range [a, b), a >= ring start."""
        out = bytearray()
        for start, pcm in self._ring:
            n = len(pcm) // 2
            if start + n <= a:
                continue
            if start >= b:
                break
            lo, hi = max(a, start), min(b, start + n)
            out += pcm[(lo - start) * 2:(hi - start) * 2]
        return bytes(out)

    # ── output ──

    def _drain_chunks(self):
        limit = self._total if self._end_target is None \
            else min(self._total, self._end_target)
        while limit - self._next_emit >= self.chunk_samples:
            pcm = self._slice(self._next_emit,
                              self._next_emit + self.chunk_samples)
            self._emit_chunk(pcm)
            self._next_emit += self.chunk_samples

    def _finalize(self):
        end = self._end_target
        avail_start = self._total - self._held
        if self.stream:
            # remaining audio as one final chunk, zero-padded to full size
            if self._next_emit < end:
                pcm = self._slice(max(self._next_emit, avail_start), end)
                short = self.chunk_samples - (end - self._next_emit)
                if short > 0:
                    pcm += b"\x00\x00" * short
                self._emit_chunk(pcm)
            self.emit_signal("vad_end", {"timestamp": self._mark_ts})
        else:
            lo = max(self._mark, avail_start)
            pcm = self._slice(lo, end)
            self.logger.info(
                f"vad segment: {(end - lo) / self.sample_rate:.2f}s "
                f"(samples {lo}..{end})"
            )
            # signal first, then the audio — same order the old collector kept
            self.emit_signal("vad_end", {"timestamp": self._mark_ts})
            output_data = {}
            self.add_output(output_data, "audio_file", self._pcm_to_wav(pcm))
            segment_pass = dict(self._start_pass)
            segment_pass["timestamp"] = self._mark_ts
            self.output_to_queue(output_data, segment_pass, is_log=False)
        self.end_span()
        self._reset_turn()

    def _emit_chunk(self, pcm):
        output_data = {}
        self.add_output(output_data, "audio_file", self._pcm_to_wav(pcm))
        self.output_to_queue(output_data, {"timestamp": self._mark_ts},
                             is_add_pass_data=False, is_log=False)

    def _pcm_to_wav(self, pcm):
        bio = io.BytesIO()
        with wave.open(bio, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(self.sample_rate)
            wf.writeframes(pcm)
        return base64.b64encode(bio.getvalue()).decode("ascii")
