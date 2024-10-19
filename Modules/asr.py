import requests
from .config import *

class TestASRCaller:
    def __init__(self):
        pass
    def call(self, audio_file):
        text = "This is an ASR test. "
        return text
    
class SenceVoiceCaller:
    def __init__(self):
        pass
    def call(self, audio_file):
        try:
            response = requests.post(addr_ASRCaller + "/asr", files={'file': audio_file})
            text = response.json()["text"]
            return text
        except Exception as e:
            print(e)
            return "error"