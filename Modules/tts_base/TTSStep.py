from pydub import AudioSegment
from io import BytesIO

from ..base.BaseProcessingStep import BaseProcessingStep
from ..utils.functions import bytes_to_base64


class BaseTTSCaller:
    def __init__(self, logger):
        self.logger = logger
        self.repeat = 2
        self.empty_audio = AudioSegment.silent(duration=10)
        pass

    def call(self, prompt, language="auto", speaker=""):
        try:
            audio_data = self.empty_audio
            if prompt != "":
                for i in range(self.repeat):
                    audio = AudioSegment.from_file("test/test_voice.wav", format="wav")
                    audio_data += audio

            # Create a BytesIO object as an in-memory file
            audio_bytes_io = BytesIO()

            # Export audio data as WAV format into the BytesIO object
            audio_data.export(audio_bytes_io, format="wav")

            # Get the byte stream
            audio_bytes = audio_bytes_io.getvalue()
            return audio_bytes
        except Exception as e:
            self.logger.error(f"failed to call tts: {e}")
            return ""

    def call_stream(self, prompt, language="auto", speaker=""):
        """Yield audio chunks (WAV bytes). Base fallback: a single chunk holding
        the full call() result, so stream mode works (degenerately) with any
        caller; real streaming callers override this."""
        result = self.call(prompt, language, speaker)
        if result:
            yield result


class TTSStep(BaseProcessingStep):
    REQUIRED_INPUTS = ["text"]

    def custom_init(self):
        self.tts_caller = BaseTTSCaller(self.logger)
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
        """Emit one message per chunk. The first chunk carries the full
        pass_vars meta — downstream sees the sentence meta exactly once, the
        same as non-stream; later chunks carry only the timestamp (so cancel
        semantics still apply to every chunk)."""
        first = True
        for chunk in self.tts_caller.call_stream(text, language, speaker):
            if self.check_cancel():
                self.logger.info("cancelled during tts stream")
                return
            if not chunk:
                continue
            try:
                chunk_b64 = bytes_to_base64(chunk)
            except Exception as e:
                self.logger.error(f"failed to convert tts chunk to base64: {e}")
                continue
            output_data = {}
            self.add_output(output_data, "audio_file", chunk_b64)
            if first:
                self.output_to_queue(output_data, pass_data)
                first = False
            else:
                self.output_to_queue(output_data, pass_data, is_add_pass_data=False)
        return
