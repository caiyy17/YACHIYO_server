import requests
from pydub import AudioSegment
from io import BytesIO

from .config import *

class TestTTSCaller:
    def __init__(self):
        self.repeat = 2
        self.empty_audio = AudioSegment.silent(duration=10)
        pass
    def call(self, prompt, language):
        try:
            audio_data = self.empty_audio
            if prompt != "":
                for i in range(self.repeat):
                    audio = AudioSegment.from_file(f"test/test_voice.wav", format="wav")
                    audio_data += audio

            # 创建 BytesIO 对象作为内存中的文件
            audio_bytes_io = BytesIO()

            # 将音频数据导出为 WAV 格式，并写入 BytesIO 对象
            audio_data.export(audio_bytes_io, format="wav")

            # 获取字节流
            audio_bytes = audio_bytes_io.getvalue()
            return audio_bytes
        except Exception as e:
            print(e)
            return self.empty_audio
        
class BertVitsCaller:
    def __init__(self):
        # silent 0.01s audio
        self.empty_audio = AudioSegment.silent(duration=10)
        pass
    def call(self, prompt, language):
        try:
            if prompt == "":
                audio_data = self.empty_audio
            else:
                audio = requests.post(addr_TTSCaller + "/tts", json={
                    "text": prompt,
                    "text_language": language
                })
                audio_data = []
                for chunk in audio.iter_content(chunk_size=8192): 
                    if chunk:
                        audio_data.append(chunk)
                audio_data = AudioSegment.from_file(BytesIO(b''.join(audio_data)), format="wav") + self.empty_audio

            # 创建 BytesIO 对象作为内存中的文件
            audio_bytes_io = BytesIO()

            # 将音频数据导出为 WAV 格式，并写入 BytesIO 对象
            audio_data.export(audio_bytes_io, format="wav")

            # 获取字节流
            audio_bytes = audio_bytes_io.getvalue()
            return audio_bytes
        except Exception as e:
            print(e)
            return self.empty_audio