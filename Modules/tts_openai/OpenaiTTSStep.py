import json
from pydub import AudioSegment
from io import BytesIO

from ..tts_base.TTSStep import TTSStep


class OpenaiTTSCaller:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.empty_audio = AudioSegment.silent(duration=10)
        self.client = self._create_client()
        # Pipeline config voice overrides model config voice
        self.default_voice = (
            self.config.get("voice", "") or self.model_config.get("voice", "alloy")
        )
        self._init_call()

    def _create_client(self):
        config_name = self.config.get("model", "openai")
        with open(f"configs/tts/{config_name}.json", "r") as f:
            self.model_config = json.load(f)
        self.logger.info(f"TTS Model Config: {self.model_config}")

        from openai import OpenAI
        from utils.settings import get_setting, get_secret

        api_base = self.model_config.get("api_base", "")
        api_key = self.model_config.get("api_key", "")

        if api_base == "":
            api_base = None
        else:
            api_base = get_setting("tts", api_base)

        if api_key == "":
            api_key = "EMPTY"
        else:
            api_key = get_secret(api_key)

        return OpenAI(api_key=api_key, base_url=api_base)

    def _init_call(self):
        """Init call to trigger server-side model loading if needed."""
        try:
            model_name = self.model_config.get("model_name", "tts-1")
            extra = self.model_config.get("extra", {})
            self.client.audio.speech.create(
                model=model_name,
                voice=self.default_voice,
                input="test",
                response_format="wav",
                **extra,
            )
            self.logger.info(f"TTS init call OK, voice={self.default_voice}")
        except Exception as e:
            self.logger.error(f"TTS init call failed: {e}")

    def call(self, prompt, language="auto", speaker=""):
        try:
            if prompt == "":
                audio_data = self.empty_audio
            else:
                model_name = self.model_config.get("model_name", "tts-1")
                extra = self.model_config.get("extra", {})
                voice = speaker if speaker else self.default_voice

                response = self.client.audio.speech.create(
                    model=model_name,
                    voice=voice,
                    input=prompt,
                    response_format="wav",
                    **extra,
                )

                audio_data = (
                    AudioSegment.from_file(BytesIO(response.content), format="wav")
                    + self.empty_audio
                )

            audio_bytes_io = BytesIO()
            audio_data.export(audio_bytes_io, format="wav")
            return audio_bytes_io.getvalue()
        except Exception as e:
            self.logger.error(f"failed to call tts: {e}")
            return ""


class OpenaiTTSStep(TTSStep):
    def custom_init(self):
        self.init_timeline_config()
        self.tts_caller = OpenaiTTSCaller(self.config, self.logger)
