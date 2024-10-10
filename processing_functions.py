import time
from pydub import AudioSegment
from PIL import Image
from io import BytesIO
import base64
import os
import json

def audio_to_base64(audio):
    buffer = BytesIO()
    audio.export(buffer, format="wav")
    buffer.seek(0)
    # 编码音频文件为 Base64
    audio_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
    return audio_base64

def image_to_base64(image):
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    buffer.seek(0)
    # 编码图片文件为 Base64
    image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
    return image_base64

from Modules.cut_utils import detect_language, cut_prompt_motion, get_text
from Modules.llm import ChatgptCaller as LLMCaller
from Modules.image import Dalle3Caller as ImageGenerator
from Modules.asr import WhisperCaller as ASRCaller
from Modules.tts import BertVitsCaller as TTSCaller
from Modules.t2e import T2ECaller as T2ECaller
llm_caller = LLMCaller()
image_generator = ImageGenerator()
asr_caller = ASRCaller()
tts_caller = TTSCaller()
t2e_caller = T2ECaller()

import queue
import threading

def call_llm_queue(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
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
            data = input_queue.get(timeout=1)
            print("call_llm_queue", data)
            prompt = data
            # 处理数据
            answer = ""
            for response in llm_caller.call_stream(prompt, id):
                if cancel_event.is_set():
                    break
                answer += response
                output_queue.put(response)
            output_queue.put("[EoS]")
        except queue.Empty:
            pass

def llm_text_process_queue(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
    length_threshold = 1
    language = "zh"
    accumulated_text = ""
    while True:
        if kill_event.is_set():
            break
        if cancel_event.is_set():
            while not input_queue.empty():
                input_queue.get()
            next_cancel_event.set()
            cancel_event.clear()
            accumulated_text = ""
        try:
            data = input_queue.get(timeout=1)
            prompt = data
            if prompt == "[EoS]":
                if accumulated_text != "":
                    prompts = cut_prompt_motion(accumulated_text, language, length_threshold)
                    for p in prompts:
                        output_queue.put(p)
                output_queue.put("[EoS]")
                break
            accumulated_text += prompt
            prompts = cut_prompt_motion(accumulated_text, language, length_threshold)
            accumulated_text = prompts[-1][0]
            for p in prompts[:-1]:
                output_queue.put(p)
        except queue.Empty:
            pass

def call_tts_queue(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
    index = 0
    emotion = ""
    motion = ""
    interrupt = False
    while True:
        if kill_event.is_set():
            break
        if cancel_event.is_set():
            while not input_queue.empty():
                input_queue.get()
            next_cancel_event.set()
            cancel_event.clear()
            index = 0
            emotion = ""
            motion = ""
            interrupt = False
        try:
            data = input_queue.get(timeout=1)
            prompt = data
            if prompt == "[EoS]":
                output_queue.put("[EoS]")
                break
            if prompt[1] == "motion":
                motion = prompt[0]
                print("Motion: ", motion)
                emotion = t2e_caller.call(prompt[0], "zh")
                interrupt = True
                continue
            print("TTS: " + prompt[0])
            if get_text(prompt[0]) == "":
                audio = ""
            else:
                audio = tts_caller.call(prompt[0], prompt[1])
            output_queue.put([motion + prompt[0], audio, emotion, interrupt])

            motion = ""
            emotion = ""
            interrupt = False
            index += 1
        except queue.Empty:
            pass

def prepare_response_queue(send_queue, input_queue, output_queue, cancel_event, next_cancel_event, kill_event):
    combined = AudioSegment.empty()
    index = 0
    while True:
        if kill_event.is_set():
            break
        if cancel_event.is_set():
            while not input_queue.empty():
                input_queue.get()
            next_cancel_event.set()
            cancel_event.clear()
        try:
            data = input_queue.get(timeout=1)
            response = data
            if response == "[EoS]":
                output_queue.put("[EoS]")
                combined.export("tmp/answer.wav", format="wav")
                break
            p = response[0]
            print("Response: ", p)
            if response[1] != "" and response[1] != "error":
                audio = AudioSegment.empty()
                audio += response[1]
                emotion = response[2]
                interrupt = response[3]
                combined += audio
                audio_base64 = audio_to_base64(audio)
            else:
                audio = AudioSegment.empty()
                emotion = ""
                interrupt = False
                audio_base64 = ""
            
            result = json.dumps({'text': p, 'emotion': emotion, 'index': index, 'audio': audio_base64, 'type': '[audio]', 'interrupt': interrupt}) + '\n'
            output_queue.put(result)
            index += 1
        except queue.Empty:
            pass

# 定义函数映射字典
FUNCTION_MAP = {
    "call_llm_queue": call_llm_queue,
    "llm_text_process_queue": llm_text_process_queue,
    "call_tts_queue": call_tts_queue,
    "prepare_response_queue": prepare_response_queue,
    # 添加其他函数
}

def get_function_by_name(function_name):
    return FUNCTION_MAP.get(function_name)