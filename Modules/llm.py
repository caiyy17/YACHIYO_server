import os
import json
import requests

from .config import *

class ChatgptCaller:
    def __init__(self):
        from openai import OpenAI
        from .secrets_chatgpt import API_KEY
        self.client = OpenAI(api_key=API_KEY)
        self.model = "gpt-4o"

    def load_history(self, id):
        if os.path.exists(f"tmp/history_{id}.json"):
            with open(f"tmp/history_{id}.json", 'r', encoding='utf-8') as file:
                history = json.load(file)
        else:
            history = []
        return history
    
    def save_history(self, history, id):
        with open(f"tmp/history_{id}.json", 'w', encoding='utf-8') as file:
            json.dump(history, file, ensure_ascii=False)
    
    def clear_history(self, id):
        if os.path.exists(f"tmp/history_{id}.json"):
            os.remove(f"tmp/history_{id}.json")

    def set_system_prompt(self, system_prompt, id):
        # set system prompt will reset the history
        history = []
        history.append({"role": "system", "content": f"{system_prompt}"})
        self.save_history(history, id)
        return history
    
    def call(self, prompt, id):
        try:
            history = self.load_history(id)
            response, history = self.call_model(prompt, history)
            print(response)
            self.save_history(history, id)
            return response
        except Exception as e:
            print(e)
            return "error"
        
    def call_stream(self, prompt, id):
        try:
            history = self.load_history(id)
            accumulated_text = ""
            for response, new_history in self.call_model_stream(prompt, history):
                accumulated_text += response
                # print(response, end="", flush=True)
                yield response
            response = accumulated_text
            history = new_history
            # print("")
            print(response)
            self.save_history(history, id)
        except Exception as e:
            print(e)
            return "error"
        
    def call_model(self, prompt, history):
        history.append({"role": "user", "content": f"{prompt}"})
        try:
            result = self.client.chat.completions.create(
                model=self.model,
                messages=history
            )
            response = result.choices[0].message.content.strip()
            history.append({"role": "assistant", "content": f"{response}"})
            return response, history
        except Exception as e:
            print(e)
            return "error"
        
    def call_model_stream(self, prompt, history):
        history.append({"role": "user", "content": f"{prompt}"})
        try:
            result = self.client.chat.completions.create(
                model=self.model,
                messages=history,
                stream = True
            )
            accumulated_text = ""
            for chunk in result:
                text = chunk.choices[0].delta.content
                if text is None:
                    continue
                response = text
                accumulated_text += response
                current_history = history.copy()
                current_history.append({"role": "assistant", "content": f"{accumulated_text}"})
                yield response, current_history
        except Exception as e:
            print(e)
            yield "error"

class ChatglmCaller:
    def __init__(self):
        pass

    def load_history(self, id):
        if os.path.exists(f"tmp/history_{id}.json"):
            with open(f"tmp/history_{id}.json", 'r', encoding='utf-8') as file:
                history = json.load(file)
        else:
            history = []
        return history
    
    def save_history(self, history, id):
        with open(f"tmp/history_{id}.json", 'w', encoding='utf-8') as file:
            json.dump(history, file, ensure_ascii=False)

    def clear_history(self, id):
        if os.path.exists(f"tmp/history_{id}.json"):
            os.remove(f"tmp/history_{id}.json")
    
    def set_system_prompt(self, system_prompt, id):
        # set system prompt will reset the history
        history = []
        history.append({"role": "system", "content": f"{system_prompt}"})
        self.save_history(history, id)
        return history

    def call(self, prompt, id):
        try:
            history = self.load_history(id)
            response, history = self.call_model(prompt, history)
            print(response)
            self.save_history(history, id)
            return response
        except Exception as e:
            print(e)
            return "error"
        
    def call_stream(self, prompt, id):
        try:
            history = self.load_history(id)
            accumulated_text = ""
            for response, new_history in self.call_model_stream(prompt, history):
                accumulated_text += response
                # print(response, end="", flush=True)
                yield response
            response = accumulated_text
            history = new_history
            # print("")
            print(response)
            self.save_history(history, id)
        except Exception as e:
            print(e)
            return "error"
        
    def call_model(self, prompt, history):
        try:
            result = requests.post(addr_ChatglmCaller + "/chatglm", json={'prompt': prompt, 'history': history})
            response = result.json()["response"]
            history = result.json()["history"]
            return response, history
        except Exception as e:
            print(e)
            return "error"
        
    def call_model_stream(self, prompt, history):
        try:
            result = requests.post(addr_ChatglmCaller + "/chatglm_stream", json={'prompt': prompt, 'history': history}, stream=True)
            for line in result.iter_lines():
                if line:
                    parsed_line = json.loads(line.decode('utf-8'))
                    response = parsed_line["response"]
                    history = parsed_line["history"]
                    yield response, history
        except Exception as e:
            print(e)
            yield "error"
