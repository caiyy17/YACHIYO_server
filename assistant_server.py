from flask import Flask, request, jsonify, Response
import json
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 1024 * 1024 * 1024

import time
from pydub import AudioSegment
from PIL import Image
from io import BytesIO
import base64

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

from Modules.cut_utils import detect_language, language_process, get_processed
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

def call_llm_queue(prompt, id, output_queue):
    answer = ""
    for response in llm_caller.call_stream(prompt, id):
        if cancel_tokens[id].is_set():
            break
        answer += response
        output_queue.put(response)
    output_queue.put("[EoS]")
    return answer

def llm_text_process(input_queue, output_queue, id, mark = [".", "!", "?", "。", "！", "？"]):
    accumulated_text = ""
    language = None
    cut_length = 10
    # 先收集15个字符，然后判断语言
    while True:
        if cancel_tokens[id].is_set():
            break
        prompt = input_queue.get()
        if prompt == "[EoS]":
            if accumulated_text != "":
                if language is None:
                    language = detect_language(accumulated_text)
                # print("OUT: " + accumulated_text + " [" + language + "]")
                output_queue.put([accumulated_text, language])
            output_queue.put("[EoS]")
            break
        accumulated_text += prompt
        if len(accumulated_text) > cut_length and accumulated_text[-1] in mark:
            if language is None:
                language = detect_language(accumulated_text)
                if language == "zh":
                    cut_length = 30
                else:
                    cut_length = 100
                # 第一句话
                processed = language_process(accumulated_text, language)
                for p in processed:
                    # print("OUT: " + p + " [" + language + "]")
                    output_queue.put([p, language])
                accumulated_text = ""
            else:
                processed, accumulated_text = get_processed(accumulated_text, language)
                for p in processed:
                    # print("OUT: " + p + " [" + language + "]")
                    output_queue.put([p, language])

def call_tts_queue(input_queue, output_queue, id):
    index = 0
    while True:
        if cancel_tokens[id].is_set():
            break
        prompt = input_queue.get()
        if prompt == "[EoS]":
            output_queue.put("[EoS]")
            break
        print("TTS: " + prompt[0])
        # if index > 0:
        #     emotion = t2e_caller.call(prompt[0], prompt[1])
        # else:
        #     emotion = "neutral"
        emotion = t2e_caller.call(prompt[0], prompt[1])
        audio = tts_caller.call(prompt[0], prompt[1])
        output_queue.put([prompt[0], audio, emotion])
        index += 1

@app.route('/llm', methods=['POST'])
def llm():
    start = time.time()
    data = request.json
    prompt = data['prompt']
    id = data['id']
    print("Prompt: ", prompt)
    try:
        answer = llm_caller.call(prompt, id)
        print("Time: ", time.time() - start)
        # 将回答写入JSON文件
        with open('tmp/answer.json', 'w', encoding='utf-8') as file:
            json.dump({'prompt': prompt, 'answer': answer}, file, ensure_ascii=False)
        return jsonify({'answer': answer})
    except Exception as e:
        print(e)
        return jsonify({'error': "llm error"})
    
@app.route('/llm_stream', methods=['POST'])
def llm_stream():
    start = time.time()
    data = request.json
    prompt = data['prompt']
    id = data['id']
    print("Prompt: ", prompt)
    def generate(prompt, id):
        answer = ""
        try:
            llm_output_queue = queue.Queue()
            text_process_quene = queue.Queue()
            # start the thread
            threading.Thread(target=call_llm_queue, args=(prompt, id, llm_output_queue)).start()
            threading.Thread(target=llm_text_process, args=(llm_output_queue, text_process_quene, id)).start()
            while True:
                response = text_process_quene.get()
                if response == "[EoS]":
                    break
                answer += response[0]
                yield json.dumps({'answer': response[0]}) + '\n'
                print("Time: ", time.time() - start)
            # 将回答写入JSON文件
            with open('tmp/answer.json', 'w', encoding='utf-8') as file:
                json.dump({'prompt': prompt, 'answer': answer}, file, ensure_ascii=False)
            yield json.dumps({'end': "[EoS]"}) + '\n'
        except Exception as e:
            print(e)
            yield json.dumps({'error': "llm error"}) + '\n'
    return Response(generate(prompt, id), content_type='application/json')

@app.route('/tts', methods=['POST'])
def tts():
    start = time.time()
    data = request.json
    prompt = data['prompt']
    language = data['language']

    # detect language
    language = detect_language(prompt)
    prompts = language_process(prompt, language)

    combined = AudioSegment.empty()
    try:
        for p in prompts:
            print("TTS: " + p)
            audio = tts_caller.call(p, language)
            combined += audio
            print("Time: ", time.time() - start)
        combined.export("tmp/answer.wav", format="wav")
        audio_base64 = audio_to_base64(combined)
        return jsonify({'audio': audio_base64, 'text': prompts})
    except Exception as e:
        print(e)
        return jsonify({'error': "tts error"})

@app.route('/asr', methods=['POST'])
def asr():
    start = time.time()
    audio_file = request.files['file']
    print("Transcribe: ")
    try:
        filename = 'tmp/received_file.wav'
        audio_file.save(filename)
        with open(filename, 'rb') as audio_file:
            text = asr_caller.call(audio_file)
        print("Time: ", time.time() - start)
        # 将回答写入JSON文件
        with open('tmp/transcript.json', 'w', encoding='utf-8') as file:
            json.dump({'answer': text}, file, ensure_ascii=False)
        return jsonify({'answer': text})
    except Exception as e:
        print(e)
        return jsonify({'error': "asr error"})
    
@app.route('/image', methods=['POST'])
def image():
    start = time.time()
    data = request.json
    prompt = data['prompt']
    print("Image: ", prompt)
    try:
        image = image_generator.call(prompt)
        print("Time: ", time.time() - start)
        image = Image.open(image)
        image.save('tmp/answer.png')
        image_base64 = image_to_base64(image)
        return jsonify({'image': image_base64, 'text': prompt})
    except Exception as e:
        print(e)
        return jsonify({'error': "image error"})

@app.route('/llm_tts', methods=['POST'])
def llm_tts():
    start = time.time()
    data = request.json
    prompt = data['prompt']
    id = data['id']
    print("Prompt: ", prompt)

    if id not in cancel_tokens:
        cancel_tokens[id] = CancelToken()
    cancel_tokens[id].clear()

    def generate(prompt, id):
        try:
            llm_output_queue = queue.Queue()
            tts_input_queue = queue.Queue()
            tts_output_queue = queue.Queue()
            # start the thread
            threading.Thread(target=call_llm_queue, args=(prompt, id, llm_output_queue)).start()
            threading.Thread(target=llm_text_process, args=(llm_output_queue, tts_input_queue, id)).start()
            threading.Thread(target=call_tts_queue, args=(tts_input_queue, tts_output_queue, id)).start()

            print("Time: ", time.time() - start)
            combined = AudioSegment.empty()
            index = 0
            while True:
                response = tts_output_queue.get()
                if response == "[EoS]":
                    break
                p = response[0]
                audio = AudioSegment.empty()
                audio += response[1]
                emotion = response[2]
                print("Time: ", time.time() - start, " text: ", p, 'index: ', index)
                combined += audio
                audio_base64 = audio_to_base64(audio)
                yield json.dumps({'text': p, 'emotion': emotion, 'index': index, 'audio': audio_base64}) + '\n'
                index += 1
            combined.export("tmp/answer.wav", format="wav")
            yield json.dumps({'end': "[EoS]"}) + '\n'
        except Exception as e:
            print(e)
            yield json.dumps({'error': "llm_tts error"}) + '\n'
    return Response(generate(prompt, id), content_type='application/json')

@app.route('/asr_llm_tts', methods=['POST'])
def asr_llm_tts():
    start = time.time()
    audio_file = request.files['file']
    id = request.form.get('id')
    try:
        filename = 'tmp/received_file.wav'
        audio_file.save(filename)
        time.sleep(0.1)
        with open(filename, 'rb') as audio_file:
            text = asr_caller.call(audio_file)
        print("asr time: ", time.time() - start)
    except Exception as e:
        print(e)
        return jsonify({'error': "asr error"})
    
    prompt = text
    print("Prompt: ", prompt)

    if id not in cancel_tokens:
        cancel_tokens[id] = CancelToken()
    cancel_tokens[id].clear()

    def generate(prompt, id):
        try:
            ##############################
            # RAG part start
            ##############################
            from RAG.rag import rag_call2 as rag_call
            prompt, image = rag_call(prompt)
            yield json.dumps({'image': image, 'type': "[im]"}) + '\n'
            # yield json.dumps(payload) + '\n'

            # prompt开头是[NOT FOUND]
            if prompt.startswith("[NOT FOUND]"):
                prompt = prompt[11:]
                response = tts_caller.call(prompt, 'zh')
                audio = AudioSegment.empty()
                audio += response
                audio_base64 = audio_to_base64(audio)
                yield json.dumps({'text': prompt, 'emotion': 'sadness', 'index': 0, 'audio': audio_base64, 'type': '[audio]'}) + '\n'
                yield json.dumps({'end': "[EoS]"}) + '\n'
                return
            ##############################
            # Rag part end
            ##############################
            llm_output_queue = queue.Queue()
            tts_input_queue = queue.Queue()
            tts_output_queue = queue.Queue()

            # start the thread
            llm_thread = threading.Thread(target=call_llm_queue, args=(prompt, id, llm_output_queue))
            llm_tts_thread = threading.Thread(target=llm_text_process, args=(llm_output_queue, tts_input_queue, id))
            tts_thread = threading.Thread(target=call_tts_queue, args=(tts_input_queue, tts_output_queue, id))
            
            llm_thread.start()
            tts_thread.start()
            llm_tts_thread.start()

            print("Time: ", time.time() - start)
            combined = AudioSegment.empty()
            index = 0
            while True:
                if cancel_tokens[id].is_set():  # 检查取消信号
                    llm_thread.join()
                    tts_thread.join()
                    llm_tts_thread.join()
                    print("Canceled")
                    yield json.dumps({'end': "[EoS]"}) + '\n'
                    break

                response = tts_output_queue.get()
                if response == "[EoS]":
                    break
                p = response[0]
                audio = AudioSegment.empty()
                audio += response[1]
                emotion = response[2]
                print("Time: ", time.time() - start, " text: ", p, 'index: ', index)
                combined += audio
                audio_base64 = audio_to_base64(audio)
                yield json.dumps({'text': p, 'emotion': emotion, 'index': index, 'audio': audio_base64, 'type': '[audio]'}) + '\n'
                index += 1
            combined.export("tmp/answer.wav", format="wav")
            yield json.dumps({'end': "[EoS]"}) + '\n'
        except Exception as e:
            print(e)
            yield json.dumps({'error': "llm_tts error"}) + '\n'
        finally:
            cancel_tokens[id].clear()
    return Response(generate(prompt, id), content_type='application/json')

@app.route('/t2e', methods=['POST'])
def t2e():
    start = time.time()
    data = request.json
    prompt = data['prompt']
    language = data['language']
    print("T2E: ", prompt)
    try:
        emotion = t2e_caller.call(prompt, language)
        print(emotion)
        print("Time: ", time.time() - start)
        return jsonify({'emotion': emotion, 'text': prompt})
    except Exception as e:
        print(e)
        return jsonify({'error': "t2e error"})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5050)