import requests
from .config import *

import json

class TestMotionCaller:
    def __init__(self):
        pass
    def call(self, prompt):
        motion = "motion processed. " + prompt
        return motion
    
class BGEMotionCaller:
    def __init__(self, dataset=""):
        pass

    def init_dataset(self, dataset):
        try:
            response_init = requests.post(addr_MotionCaller + "/init", json={
                "dataset": dataset
            })
            return response_init.text.strip("\n")
        except Exception as e:
            print(e)
            return "error"
        
    def call(self, prompt):
        try:
            response = requests.post(addr_MotionCaller + "/get_action", json={
                "query": prompt
            })
            motion = response.json()["motion"]
            return motion
        except Exception as e:
            print(e)
            return "error"