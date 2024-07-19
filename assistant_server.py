from flask import Flask, request, jsonify, Response
import json
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024

import time
from pydub import AudioSegment
from PIL import Image
from io import BytesIO
import base64
import os

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

from Modules.cut_utils import detect_language, cut_prompt, get_text
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
cancel_tokens = {}

class CancelToken:
    def __init__(self):
        self._cancel_event = threading.Event()
        self.time = time.time()

    def set(self):
        self._cancel_event.set()

    def is_set(self):
        return self._cancel_event.is_set()
    
    def clear(self):
        self._cancel_event.clear()

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    return jsonify({'status': 'alive'})

@app.route('/cancel', methods=['POST'])
def cancel():
    data = request.json
    id = data['id']
    if(id not in cancel_tokens):
        return jsonify({'error': 'id not found'})
    cancel_tokens[id].set()
    return jsonify({'status': 'cancelled'})

@app.route('/clear', methods=['POST'])
def clear():
    data = request.json
    id = data['id']
    llm_caller.clear_history(id)
    return jsonify({'status': 'history cleared'})

@app.route('/set_system_prompt', methods=['POST'])
def set_system_prompt():
    data = request.json
    system_prompt = data['system_prompt']
    id = data['id']
    if system_prompt == "":
        return jsonify({'error': "system prompt is empty"})
    llm_caller.set_system_prompt(system_prompt, id)
    return jsonify({'status': 'system prompt set'})

@app.route('/set_model', methods=['POST'])
def set_model():
    data = request.json
    print(data)
    if 'model' not in data:
        print("Model not changed")
        return jsonify({'status': "model not changed"})
    model = data['model']
    config = data['config']
    speaker = data['speaker']
    tts_caller.change_model(model, config, speaker)
    return jsonify({'status': "model changed"})

def call_llm_queue(send_queue, prompt, output_queue, id):
    answer = ""
    for response in llm_caller.call_stream(prompt, id):
        if cancel_tokens[id].is_set():
            break
        answer += response
        output_queue.put(response)
    output_queue.put("[EoS]")
    return answer

def llm_text_process(send_queue, input_queue, output_queue, id):
    length_threshold = 1
    language = "zh"
    accumulated_text = ""
    while True:
        if cancel_tokens[id].is_set():
            break
        prompt = input_queue.get()
        if prompt == "[EoS]":
            if accumulated_text != "":
                prompts = cut_prompt(accumulated_text, language, length_threshold)
                for p in prompts:
                    output_queue.put(p)
            output_queue.put("[EoS]")
            break
        accumulated_text += prompt
        prompts = cut_prompt(accumulated_text, language, length_threshold)
        accumulated_text = prompts[-1][0]
        for p in prompts[:-1]:
            output_queue.put(p)

def call_tts_queue(send_queue, input_queue, output_queue, id):
    index = 0
    while True:
        if cancel_tokens[id].is_set():
            break
        prompt = input_queue.get()
        if prompt == "[EoS]":
            output_queue.put("[EoS]")
            break
        print("TTS: " + prompt[0])
        if index == 0:
            emotion = t2e_caller.call(prompt[0], prompt[1])
            interrupt = False
        else:
            emotion = t2e_caller.call(prompt[0], prompt[1])
            interrupt = False

        audio = tts_caller.call(prompt[0], prompt[1])
        output_queue.put([prompt[0], audio, emotion, interrupt])
        index += 1

def prepare_response(send_queue, input_queue, output_queue, id):
    combined = AudioSegment.empty()
    index = 0
    while True:
        if cancel_tokens[id].is_set():
            break
        response = input_queue.get()
        if response == "[EoS]":
            break
        p = response[0]
        try:
            if get_text(p) != "" and response[1] != "error":
                audio = AudioSegment.empty()
                audio += response[1]
                emotion = response[2]
                interrupt = response[3]
            else:
                audio = AudioSegment.empty()
                emotion = "none"
                interrupt = False
            combined += audio
            audio_base64 = audio_to_base64(audio)
            print("Response: ", p)
            result = json.dumps({'text': p, 'emotion': emotion, 'index': index, 'audio': audio_base64, 'type': '[audio]', 'interrupt': interrupt}) + '\n'
            output_queue.put(result)
            index += 1
        except Exception as e:
            print(e)
    output_queue.put("[EoS]")
    combined.export("tmp/answer.wav", format="wav")

def get_asr(filename, id):
    with open(filename, 'rb') as audio_file:
        text = asr_caller.call(audio_file)
    # 将回答写入JSON文件
    with open(f'tmp/transcript_{id}.json', 'w', encoding='utf-8') as file:
        json.dump({'answer': text}, file, ensure_ascii=False)
    return text

@app.route('/asr', methods=['POST'])
def asr():
    start = time.time()
    audio_file = request.files['file']
    id = request.form.get('id')
    print("Transcribe: ")
    try:
        filename = f'tmp/received_file_{id}.wav'
        audio_file.save(filename)
        text = get_asr(filename, id)
        print("ASR Time: ", time.time() - start)
        return jsonify({'answer': text})
    except Exception as e:
        print(e)
        return jsonify({'error': "asr error"})

def get_rag(send_quene, prompt, id):
    from RAG.rag import rag_call2 as rag_call
    prompt, image = rag_call(prompt)
    image_response = json.dumps({'image': image, 'type': "[im]"}) + '\n'
    send_quene.put(image_response)
    if prompt.startswith("[NOT FOUND]"):
        prompt = prompt[11:]
        response = tts_caller.call(prompt, 'zh')
        audio = AudioSegment.empty()
        audio += response
        audio_base64 = audio_to_base64(audio)
        send_quene.put(json.dumps({'text': prompt, 'emotion': 'sadness', 'index': 0, 'audio': audio_base64, 'type': '[audio]'}) + '\n')
        send_quene.put("[EoS]")
        return
    return prompt

def process_llm_tts_queue(send_quene, prompt, id):
    try:
        print("start process_llm_tts_queue")
        ##############################
        # RAG part start
        ##############################
        prompt = get_rag(send_quene, prompt, id)
        ##############################
        # Rag part end
        ##############################
        llm_output_queue = queue.Queue()
        llm_thread = threading.Thread(target=call_llm_queue, args=(send_quene, prompt, llm_output_queue, id))
        llm_thread.start()

        tts_input_queue = queue.Queue()
        llm_tts_thread = threading.Thread(target=llm_text_process, args=(send_quene, llm_output_queue, tts_input_queue, id))
        llm_tts_thread.start()

        tts_output_queue = queue.Queue()
        tts_thread = threading.Thread(target=call_tts_queue, args=(send_quene, tts_input_queue, tts_output_queue, id))
        tts_thread.start()

        response_output_queue = queue.Queue()
        response_thread = threading.Thread(target=prepare_response, args=(send_quene, tts_output_queue, response_output_queue, id))
        response_thread.start()

        print("Process Start Time: ", time.time() - cancel_tokens[id].time)
        index = 0
        while True:
            if cancel_tokens[id].is_set():  # 检查取消信号
                llm_thread.join()
                tts_thread.join()
                llm_tts_thread.join()
                response_thread.join()
                break

            response = response_output_queue.get()
            if response == "[EoS]":
                break
            print("Time: ", time.time() - cancel_tokens[id].time, 'index: ', index)
            send_quene.put(response)
            index += 1
        send_quene.put("[EoS]")
    except Exception as e:
        print(e)
        send_quene.put(json.dumps({'error': "llm_tts error"}) + '\n')
    finally:
        if llm_thread.is_alive():
            llm_thread.join()
        if tts_thread.is_alive():
            tts_thread.join()
        if llm_tts_thread.is_alive():
            llm_tts_thread.join()

@app.route('/asr_llm_tts', methods=['POST'])
def asr_llm_tts():
    audio_file = request.files['file']
    id = request.form.get('id')
    
    if id not in cancel_tokens:
        cancel_tokens[id] = CancelToken()
    cancel_tokens[id].clear()

    cancel_tokens[id].time = time.time()
    try:
        filename = f'tmp/received_file_{id}.wav'
        audio_file.save(filename)
        text = get_asr(filename, id)
        print("ASR Time: ", time.time() - cancel_tokens[id].time)
    except Exception as e:
        print(e)
        return jsonify({'error': "asr error"})
    
    prompt = text
    print("Prompt: ", prompt)

    def generate(prompt, id):

        try:
            send_quene = queue.Queue()
            process_thread = threading.Thread(target=process_llm_tts_queue, args=(send_quene, prompt, id))
            process_thread.start()

            while True:
                response = send_quene.get()
                if response == "[EoS]":
                    break
                yield response
            yield json.dumps({'end': "[EoS]"}) + '\n'
        except Exception as e:
            print(e)
            yield json.dumps({'error': "llm_tts error"}) + '\n'
        finally:
            # 如果thread没有join，就结束所有线程
            cancel_tokens[id].set()
            if process_thread.is_alive():
                process_thread.join()
            cancel_tokens[id].clear()
    return Response(generate(prompt, id), content_type='application/json')

if __name__ == '__main__':
    # 删除tmp文件夹
    if os.path.exists('tmp'):
        os.system('rm -rf tmp')
    os.makedirs('tmp')
    app.run(debug=True, host='0.0.0.0', port=5005)