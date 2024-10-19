import requests
import time

from .config import *

class T2ECaller:
    def __init__(self):
        pass

    def call(self, prompt, language):
        try:
            result = requests.post(addr_T2ECaller + "/t2e", json={'text': prompt, 'language': language})
            emotion = result.json()["emotion"]
            return emotion
        except Exception as e:
            print(e)
            return "error"
        