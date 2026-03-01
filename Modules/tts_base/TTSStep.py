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


class TTSStep(BaseProcessingStep):
    def custom_init(self):
        self.tts_caller = BaseTTSCaller(self.logger)
        self.tts_caller.call("test")

    def process(self, data, pass_data={}):
        text = data.get("text", "")
        language = data.get("language", "auto")
        speaker = data.get("speaker", "")
        text = text.strip("\n")
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
