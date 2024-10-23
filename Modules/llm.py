import os
import json
import requests

import time

from .config import *

import random
class TestLLMCaller:
    def __init__(self, sleep_time=0):
        self.sleep_time = sleep_time
        pass

    def load_history(self, id):
        if os.path.exists(f"tmp/history_{id}.json"):
            with open(f"tmp/history_{id}.json", 'r', encoding='utf-8') as file:
                try:
                    history = json.load(file)
                except:
                    history = []
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
        history = self.load_history(id)
        if len(history) > 0 and history[0]["role"] == "system":
            history[0] = {"role": "system", "content": f"{system_prompt}"}
        else:
            history = []
            history.append({"role": "system", "content": f"{system_prompt}"})
        self.save_history(history, id)
        return history
    
    def call_stream(self, prompt, id):
        try:
            history = self.load_history(id)
            accumulated_text = ""
            for response, new_history in self.call_model_stream(prompt, history):
                accumulated_text += response
                self.save_history(new_history, id)
                yield response
        except Exception as e:
            print(e)
            return "error"
        
    def call_model_stream(self, prompt, history):
        try:
            accumulated_text = ""
            current_history = history.copy()
            text_list = [
                "[test motion 1] ", 
                "This is your prompt: " + prompt + "Processed by LLM 2. \n", 
                "这是你的请求：" + prompt + "经过语言模型处理3。\n\n", 
                "\t\t[测试动作4]", 
                "\nThis is your prompt: " + prompt + "经过语言模型处理5。", 
                "这是你的请求：" + prompt + "Processed by LLM 6. ",
                "[test motion part",
            ]
            # 连接所有文本
            concatenated_text = "".join(text_list)
            # 将文本随机切分成多个部分
            while len(concatenated_text) > 0:
                text = concatenated_text[:random.randint(1, min(10, len(concatenated_text)))]
                concatenated_text = concatenated_text[len(text):]
                time.sleep(self.sleep_time)
                line = json.dumps({"response": text, "history": current_history})
                if line:
                    parsed_line = json.loads(line)
                    response = parsed_line["response"]
                    accumulated_text += text
                    current_history = history.copy()
                    current_history.append({"role": "assistant", "content": accumulated_text})
                    yield response, current_history
        except Exception as e:
            print(e)
            yield "error"

class ChatgptCaller:
    def __init__(self, sleep_time=0):
        from openai import OpenAI
        from .secrets_chatgpt import API_KEY
        self.client = OpenAI(api_key=API_KEY)
        self.model = "gpt-4o"

    def load_history(self, id):
        if os.path.exists(f"tmp/history_{id}.json"):
            with open(f"tmp/history_{id}.json", 'r', encoding='utf-8') as file:
                try:
                    history = json.load(file)
                except:
                    history = []
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
        history = self.load_history(id)
        if len(history) > 0 and history[0]["role"] == "system":
            history[0] = {"role": "system", "content": f"{system_prompt}"}
        else:
            history = []
            history.append({"role": "system", "content": f"{system_prompt}"})
        self.save_history(history, id)
        return history

    def call_stream(self, prompt, id):
        try:
            history = self.load_history(id)
            accumulated_text = ""
            for response, new_history in self.call_model_stream(prompt, history):
                accumulated_text += response
                self.save_history(new_history, id)
                yield response
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

class ChatGLMCaller:
    def __init__(self, sleep_time=0):
        pass

    def load_history(self, id):
        if os.path.exists(f"tmp/history_{id}.json"):
            with open(f"tmp/history_{id}.json", 'r', encoding='utf-8') as file:
                try:
                    history = json.load(file)
                except:
                    history = []
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
        history = self.load_history(id)
        if len(history) > 0 and history[0]["role"] == "system":
            history[0] = {"role": "system", "content": f"{system_prompt}"}
        else:
            history = []
            history.append({"role": "system", "content": f"{system_prompt}"})
        self.save_history(history, id)
        return history

    def call_stream(self, prompt, id):
        try:
            history = self.load_history(id)
            accumulated_text = ""
            for response, new_history in self.call_model_stream(prompt, history):
                accumulated_text += response
                self.save_history(new_history, id)
                yield response
        except Exception as e:
            print(e)
            return "error"

    def call_model_stream(self, prompt, history):
        history.append({"role": "user", "content": f"{prompt}"})
        try:
            url = addr_LLMCaller + "/chat"
            headers = {'Content-Type': 'application/json'}
            data = {
                'query': prompt,
                'history': history
            }
            result = requests.post(url, headers=headers, data=json.dumps(data), stream=True)
            accumulated_text = ""
            if result.status_code == 200:
                for chunk in result.iter_lines():
                    if chunk:
                        chunk = chunk.decode('utf-8')
                        chunk = json.loads(chunk)
                        # print(chunk)
                        text = chunk["response"]
                        if text is None or text == "":
                            continue
                        response = text
                        accumulated_text += response
                        current_history = history.copy()
                        current_history.append({"role": "assistant", "content": f"{accumulated_text}"})
                        yield response, current_history
        except Exception as e:
            print(e)
            yield "error"