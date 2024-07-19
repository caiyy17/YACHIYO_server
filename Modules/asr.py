import requests
from .config import *

class WhisperOpenaiCaller:
    def __init__(self):
        from openai import OpenAI
        from .secrets_chatgpt import API_KEY
        self.client = OpenAI(api_key=API_KEY)
        self.model = "whisper-1"

    def call(self, audio_file):
        try:
            transcript = self.client.audio.transcriptions.create(
                model="whisper-1", 
                file=audio_file
            )
            text = transcript.text
            print(text)
            return text
        except Exception as e:
            print(e)
            return "error"
    
class WhisperCaller:
    def __init__(self):
        pass
    def call(self, audio_file):
        try:
            response = requests.post(addr_WhisperCaller + "/asr", files={'file': audio_file})
            text = response.json()["text"]
            print(text)
            return text
        except Exception as e:
            print(e)
            return "error"
        
class TestASRCaller:
    def __init__(self):
        pass
    def call(self, audio_file):
        text = "This is a test."
        return text