import queue
import threading

TIMEOUT=1

def call_default_func(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
    while True:
        if kill_event.is_set():
            break
        if cancel_event.is_set():
            while not input_queue.empty():
                input_queue.get()
            next_cancel_event.set()
            cancel_event.clear()
        try:
            # 从 input_queue 获取数据
            data = input_queue.get(timeout=TIMEOUT)
            # 处理数据
            processed_data = data
            # 将数据放入 output_queue
            output_queue.put(processed_data)
        except queue.Empty:
            pass

def call_func_a(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
    while True:
        if kill_event.is_set():
            break
        if cancel_event.is_set():
            print("call_func_a cancel_event")
            while not input_queue.empty():
                input_queue.get()
            next_cancel_event.set()
            cancel_event.clear()
        try:
            # 从 input_queue 获取数据
            data = input_queue.get(timeout=TIMEOUT)
            # 处理数据
            print("call_func_a")
            processed_data = "call_func_a"
            # 将数据放入 output_queue
            output_queue.put(processed_data)
        except queue.Empty:
            pass

def call_func_b(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
    while True:
        if kill_event.is_set():
            break
        if cancel_event.is_set():
            print("call_func_b cancel_event")
            while not input_queue.empty():
                input_queue.get()
            next_cancel_event.set()
            cancel_event.clear()
        try:
            # 从 input_queue 获取数据
            data = input_queue.get(timeout=TIMEOUT)
            # 处理数据
            print("call_func_b")
            processed_data = "call_func_b"
            # 将数据放入 output_queue
            output_queue.put(processed_data)
        except queue.Empty:
            pass

import json
import base64

from Modules.asr import TestASRCaller
from Modules.llm import TestLLMCaller
from Modules.tts import TestTTSCaller

asr_caller = TestASRCaller()
llm_caller = TestLLMCaller()
tts_caller = TestTTSCaller()

def bytes_to_base64(bytes_data):
    return base64.b64encode(bytes_data).decode('utf-8')

def base64_to_bytes(base64_data):
    return base64.b64decode(base64_data)

def call_asr(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
    while True:
        if kill_event.is_set():
            break
        if cancel_event.is_set():
            print("call_asr cancel_event")
            while not input_queue.empty():
                input_queue.get()
            next_cancel_event.set()
            cancel_event.clear()
        try:
            # 从 input_queue 获取数据
            data = input_queue.get(timeout=TIMEOUT)
            data = json.loads(data)
            # 处理数据
            print("call_asr")
            audio_file = data["audio_file"]
            audio_file = base64_to_bytes(audio_file)
            asr_result = asr_caller.call(audio_file)
            print("asr_result: ", asr_result)
            # 从data中删去audio_file，加入asr_result
            data.pop("audio_file")
            data["prompt"] = asr_result
            data["language"] = "en-US"
            processed_data = json.dumps(data)
            # 将数据放入 output_queue
            output_queue.put(processed_data)
        except queue.Empty:
            pass

def call_llm(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
    while True:
        if kill_event.is_set():
            break
        if cancel_event.is_set():
            print("call_llm cancel_event")
            while not input_queue.empty():
                input_queue.get()
            next_cancel_event.set()
            cancel_event.clear()
        try:
            # 从 input_queue 获取数据
            data = input_queue.get(timeout=TIMEOUT)
            data = json.loads(data)
            # 处理数据
            print("call_llm")
            prompt = data["prompt"]
            id = data["id"]
            data.pop("prompt")
            for response in llm_caller.call_stream(prompt, id):
                print("llm_response: ", response)
                current_data = data.copy()
                current_data["text"] = response
                processed_data = json.dumps(current_data)
                # 将数据放入 output_queue
                output_queue.put(processed_data)
        except queue.Empty:
            pass

def call_tts(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
    while True:
        if kill_event.is_set():
            break
        if cancel_event.is_set():
            print("call_tts cancel_event")
            while not input_queue.empty():
                input_queue.get()
            next_cancel_event.set()
            cancel_event.clear()
        try:
            # 从 input_queue 获取数据
            data = input_queue.get(timeout=TIMEOUT)
            data = json.loads(data)
            # 处理数据
            print("call_tts")
            text = data["text"]
            language = data["language"]
            tts_result = tts_caller.call(text, language)
            tts_result = bytes_to_base64(tts_result)
            print("tts_result generated")
            # 将数据放入 output_queue
            data["audio_data"] = tts_result
            processed_data = json.dumps(data)
            output_queue.put(processed_data)
        except queue.Empty:
            pass

# 定义函数映射字典
FUNCTION_MAP = {
    'call_func_a': call_func_a,
    'call_func_b': call_func_b,
    'default': call_default_func,
    # 添加其他函数
    'call_asr': call_asr,
    'call_llm': call_llm,
    'call_tts': call_tts,
}

def get_function_by_name(function_name):
    result = FUNCTION_MAP.get(function_name)
    if result is None:
        result = FUNCTION_MAP.get('default')
    return result