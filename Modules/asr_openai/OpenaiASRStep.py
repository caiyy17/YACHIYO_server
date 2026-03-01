import json
from io import BytesIO

from ..asr_base.ASRStep import ASRStep


class OpenaiASRCaller:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.client = self._create_client()
        self._init_call()

    def _create_client(self):
        model_name = self.config.get("model", "openai")
        with open(f"configs/asr/{model_name}.json", "r") as f:
            self.model_config = json.load(f)
        self.logger.info(f"ASR Model Config: {self.model_config}")

        from openai import OpenAI
        from utils.settings import get_setting, get_secret

        api_base = self.model_config.get("api_base", "")
        api_key = self.model_config.get("api_key", "")

        if api_base == "":
            api_base = None
        else:
            api_base = get_setting("asr", api_base)

        if api_key == "":
            api_key = "EMPTY"
        else:
            api_key = get_secret(api_key)

        client = OpenAI(api_key=api_key, base_url=api_base)
        return client

    def _init_call(self):
        """Init call to verify ASR service is available."""
        try:
            extra = self.model_config.get("extra", {})
            model = self.model_config.get("model", "whisper-1")
            import struct
            # 0.5s silence at 16kHz mono 16bit
            num_samples = 8000
            wav_data = struct.pack(
                "<4sI4s4sIHHIIHH4sI",
                b"RIFF", 36 + num_samples * 2, b"WAVE",
                b"fmt ", 16, 1, 1, 16000, 32000, 2, 16,
                b"data", num_samples * 2,
            ) + b"\x00" * (num_samples * 2)
            audio_io = BytesIO(wav_data)
            audio_io.name = "init.wav"
            transcription = self.client.audio.transcriptions.create(
                model=model,
                file=audio_io,
                response_format="verbose_json",
                **extra,
            )
            self.logger.info(f"ASR init call OK")
        except Exception as e:
            self.logger.error(f"ASR init call failed: {e}")

    def call(self, audio_file):
        try:
            extra = self.model_config.get("extra", {})
            model = self.model_config.get("model", "whisper-1")

            audio_io = BytesIO(audio_file)
            audio_io.name = "audio.wav"

            transcription = self.client.audio.transcriptions.create(
                model=model,
                file=audio_io,
                response_format="verbose_json",
                **extra,
            )

            text = transcription.text
            language = getattr(transcription, "language", "auto")

            self.logger.info(f"ASR result: [{language}] {text}")
            return text, language
        except Exception as e:
            self.logger.error(f"Error in OpenaiASRCaller: {e}")
            return "error", "auto"


class OpenaiASRStep(ASRStep):
    def custom_init(self):
        self.asr_caller = OpenaiASRCaller(self.config, self.logger)
