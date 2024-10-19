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
    def __init__(self):
        self.actions_set = {}
        self.actions_semantics = {}
        try:
            init = requests.post(addr_MotionCaller + "/init", json={
                "action_set": self.actions_set, "actions_semantics": self.actions_semantics
            })
        except Exception as e:
            print(e)

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