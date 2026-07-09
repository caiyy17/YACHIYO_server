from pydub import AudioSegment
from io import BytesIO

from ..base.BaseProcessingStep import BaseProcessingStep
from ..utils.functions import bytes_to_base64


class BaseTTSCaller:
    def __init__(self, config, logger):
        # (config, logger) signature, aligned with BaseMotionCaller /
        # BaseVideoCaller so any base caller is joint-registerable the same way
        self.config = config
        self.logger = logger
        self.repeat = 2
        self.empty_audio = AudioSegment.silent(duration=10)

    def _build_segment(self, prompt):
        """The test clip as an AudioSegment (empty for an empty prompt)."""
        audio = self.empty_audio
        if prompt != "":
            for _ in range(self.repeat):
                audio = audio + AudioSegment.from_file("test/test_voice.wav",
                                                       format="wav")
        return audio

    @staticmethod
    def _seg_to_wav(seg):
        bio = BytesIO()
        seg.export(bio, format="wav")
        return bio.getvalue()

    def call(self, prompt, language="auto", speaker=""):
        try:
            return self._seg_to_wav(self._build_segment(prompt))
        except Exception as e:
            self.logger.error(f"failed to call tts: {e}")
            return ""

    def call_stream(self, prompt, language="auto", speaker=""):
        """Chunk the test clip into `stream_chunk_ms` WAV pieces (last may be
        shorter), one per {"audio": <WAV bytes>} — the same per-chunk shape
        the real OpenaiTTSCaller streams (which overrides this). chunk_ms is
        rounded to a 100ms multiple so the webrtc splitter packs whole groups."""
        try:
            seg = self._build_segment(prompt)
        except Exception as e:
            self.logger.error(f"failed to call tts: {e}")
            return
        raw = int(self.config.get("stream_chunk_ms", 300))
        chunk_ms = max(100, (raw // 100) * 100)
        for i in range(0, len(seg), chunk_ms):
            piece = seg[i:i + chunk_ms]
            yield {"audio": self._seg_to_wav(piece)}


class TTSStep(BaseProcessingStep):
    REQUIRED_INPUTS = ["text"]
    # Sentence-level stream envelope, emitted only in stream mode (see
    # emitted_signals). Internal names deliberately reuse SoS/EoS ("this
    # stream starts/ends"); the wire names MUST be renamed in config (e.g.
    # SoS -> tts_SoS) when the turn-level SoS/EoS also passes through this
    # node — the emit/pass wire-name clash check enforces that.
    EMIT_SIGNALS = ["SoS", "EoS"]

    @classmethod
    def emitted_signals(cls, config):
        return ["SoS", "EoS"] if config.get("stream") else []

    def custom_init(self):
        self.tts_caller = BaseTTSCaller(self.config, self.logger)
        self.tts_caller.call("test")

    def process(self, data, pass_data={}):
        text = data.get("text", "")
        language = data.get("language", "auto")
        speaker = data.get("speaker", "")
        text = text.strip("\n")

        # stream: true -> one message per audio chunk as the caller produces
        # them (config option; default off keeps the original single-message
        # behavior untouched).
        if self.get_config("stream", False):
            self._process_stream(text, language, speaker, pass_data)
            return

        tts_result = self.tts_caller.call(text, language, speaker)
        try:
            tts_result = bytes_to_base64(tts_result)
        except Exception as e:
            self.logger.error(f"failed to convert tts_result to base64: {e}")
            tts_result = ""
        # Put data into output_queue
        output_data = {}
        self.add_output(output_data, "audio_file", tts_result)
        self.output_to_queue(output_data, pass_data)
        return

    def _process_stream(self, text, language, speaker, pass_data):
        """Single-in-multi-out protocol, same shape as the LLM turn: a
        sentence-level SoS opens the chunk stream and carries the per-
        sentence pass_vars data (wrapped under "pass_data"); every chunk
        message is uniform (payload + timestamp only; cancel semantics apply
        to each); a sentence-level EoS closes the stream so downstream knows
        no more chunks are coming. On cancel the envelope is NOT closed —
        the whole turn is stale anyway."""
        start = {"timestamp": pass_data.get("timestamp")}
        wrapped = {k: v for k, v in pass_data.items() if k != "timestamp"}
        if wrapped:
            start["pass_data"] = wrapped
        self.emit_signal("SoS", start)
        for chunk in self.tts_caller.call_stream(text, language, speaker):
            if self.check_cancel():
                self.logger.info("cancelled during tts stream")
                return
            chunk = chunk.get("audio") if isinstance(chunk, dict) else None
            if not chunk:
                continue
            try:
                chunk_b64 = bytes_to_base64(chunk)
            except Exception as e:
                self.logger.error(f"failed to convert tts chunk to base64: {e}")
                continue
            output_data = {}
            self.add_output(output_data, "audio_file", chunk_b64)
            self.output_to_queue(output_data, pass_data,
                                 is_add_pass_data=False)
        self.emit_signal("EoS", {"timestamp": pass_data.get("timestamp")})
        return
