import base64
import json
import wave
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
        with open("configs/settings/tts.json", "r") as f:
            self.model_config = json.load(f)[config_name]
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
            raise  # init failure must surface (fail-fast at pipeline init)

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

    def call_stream(self, prompt, language="auto", speaker=""):
        """Real streaming via the backend's SSE endpoint (stream_format="sse"):
        each `speech.audio.delta` event carries base64 raw PCM16 (sample rate
        in the X-Sample-Rate header); deltas are re-buffered into WAV chunks
        whose duration is a multiple of 100ms, so the webrtc frame splitter
        packs each chunk into whole 20ms-frame groups with no padding gaps
        between chunks. _rechunk adds no bytes — only the sentence's final
        short tail gets group-padded by the splitter, inside the natural
        end-of-sentence silence.
        """
        if prompt == "":
            bio = BytesIO()
            self.empty_audio.export(bio, format="wav")
            yield bio.getvalue()
            return
        try:
            model_name = self.model_config.get("model_name", "tts-1")
            extra = self.model_config.get("extra", {})
            voice = speaker if speaker else self.default_voice
            chunk_ms = int(self.config.get("stream_chunk_ms", 300))
            chunk_ms = max(100, (chunk_ms // 100) * 100)  # multiples of 100ms

            with self.client.audio.speech.with_streaming_response.create(
                model=model_name,
                voice=voice,
                input=prompt,
                response_format="pcm",
                extra_body={"stream_format": "sse"},
                **extra,
            ) as response:
                sr = int(response.headers.get("x-sample-rate") or 24000)
                chunk_bytes = sr * 2 * chunk_ms // 1000  # 16-bit mono

                def pcm_deltas():
                    for line in response.iter_lines():
                        if not line or not line.startswith("data: "):
                            continue
                        event = json.loads(line[len("data: "):])
                        if event.get("type") == "speech.audio.delta":
                            yield base64.b64decode(event["audio"])
                        # speech.audio.done (usage) ends the stream

                for pcm in self._rechunk(pcm_deltas(), chunk_bytes):
                    yield self._pcm_to_wav(pcm, sr)
        except Exception as e:
            self.logger.error(f"failed to stream tts: {e}")

    @staticmethod
    def _rechunk(byte_iter, chunk_bytes):
        """Re-buffer an irregular byte stream into fixed-size chunks (plus a
        final short tail). Lossless and order-preserving regardless of how
        the transport fragments the data."""
        buf = b""
        for data in byte_iter:
            buf += data
            while len(buf) >= chunk_bytes:
                yield buf[:chunk_bytes]
                buf = buf[chunk_bytes:]
        if buf:
            yield buf

    @staticmethod
    def _pcm_to_wav(pcm_bytes, sample_rate):
        bio = BytesIO()
        with wave.open(bio, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(sample_rate)
            wf.writeframes(pcm_bytes)
        return bio.getvalue()


class OpenaiTTSStep(TTSStep):
    def custom_init(self):
        self.tts_caller = OpenaiTTSCaller(self.config, self.logger)
