import requests
from pydub import AudioSegment
from io import BytesIO

from .config import *
    
class TTSOpenaiCaller:
    def __init__(self):
        from openai import OpenAI
        from .secrets_chatgpt import API_KEY
        self.client = OpenAI(api_key=API_KEY)
        self.model = "tts-1"
        self.voice = "alloy"

    def call(self, prompt, language):
        try:
            audio = self.client.audio.speech.create(
                model=self.model,
                voice=self.voice,
                input=prompt
            )
            audio_data = AudioSegment.from_file(BytesIO(audio.content), format="mp3")
            return audio_data
        except Exception as e:
            print(e)
            return "error"
        
    def change_model(self, model_path, config_path, speaker_name):
        self.model = model_path
        self.voice = speaker_name
        
class BertVitsCaller:
    def __init__(self):
        pass

    def call(self, prompt, language):
        try:
            audio = requests.post(addr_BertVitsCaller + "/tts", json={
                "text": prompt,
                "text_language": language
            })
            audio_data = []
            for chunk in audio.iter_content(chunk_size=8192): 
                if chunk:
                    audio_data.append(chunk)
            audio_data = AudioSegment.from_file(BytesIO(b''.join(audio_data)), format="wav")
            return audio_data
        except Exception as e:
            print(e)
            return "error"
        
    def change_model(self, model_path, config_path, speaker_name):
        try:
            response = requests.post(addr_BertVitsCaller + "/change_model", json={
                "model": model_path,
                "config": config_path,
                "speaker": speaker_name
            })
            return response.json()
        except Exception as e:
            print(e)
            return "error"