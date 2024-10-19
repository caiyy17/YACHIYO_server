import requests
from .config import *

class TestRAGCaller:
    def __init__(self, rag_info):
        self.rag_info = rag_info
        pass
    def call(self, prompt, language):
        text = self.rag_info + prompt
        image = "rag_image"
        return text, image