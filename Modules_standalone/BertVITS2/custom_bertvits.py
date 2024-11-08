# flake8: noqa: E402
import gc
import os
import logging
import re_matching
from tools.sentence import split_by_language

logging.getLogger("numba").setLevel(logging.WARNING)
logging.getLogger("markdown_it").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("matplotlib").setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.INFO, format="| %(name)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

import torch
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
import nltk
nltk.download('cmudict')
nltk.download('averaged_perceptron_tagger_eng')
import utils
from infer import infer, latest_version, get_net_g, infer_multilang
import gradio as gr
import webbrowser
import numpy as np
from config import Config
from tools.translate import translate
import librosa

net_g = None

DEFAULT_CONFIG = "config.yml"
config = Config(DEFAULT_CONFIG)
device = config.webui_config.device
if device == "mps":
    os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"
print(f"Using device: {device}")


def free_up_memory():
    # Prior inference run might have large variables not cleaned up due to exception during the run.
    # Free up as much memory as possible to allow this run to be successful.
    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()


def generate_audio(
    slices,
    sdp_ratio,
    noise_scale,
    noise_scale_w,
    length_scale,
    speaker,
    language,
    reference_audio,
    emotion,
    style_text,
    style_weight,
    skip_start=False,
    skip_end=False,
):
    audio_list = []
    # silence = np.zeros(hps.data.sampling_rate // 2, dtype=np.int16)

    free_up_memory()

    with torch.no_grad():
        for idx, piece in enumerate(slices):
            skip_start = idx != 0
            skip_end = idx != len(slices) - 1
            audio = infer(
                piece,
                reference_audio=reference_audio,
                emotion=emotion,
                sdp_ratio=sdp_ratio,
                noise_scale=noise_scale,
                noise_scale_w=noise_scale_w,
                length_scale=length_scale,
                sid=speaker,
                language=language,
                hps=hps,
                net_g=net_g,
                device=device,
                skip_start=skip_start,
                skip_end=skip_end,
                style_text=style_text,
                style_weight=style_weight,
            )
            audio16bit = gr.processing_utils.convert_to_16_bit_wav(audio)
            audio_list.append(audio16bit)
    return audio_list


def generate_audio_multilang(
    slices,
    sdp_ratio,
    noise_scale,
    noise_scale_w,
    length_scale,
    speaker,
    language,
    reference_audio,
    emotion,
    skip_start=False,
    skip_end=False,
):
    audio_list = []
    # silence = np.zeros(hps.data.sampling_rate // 2, dtype=np.int16)

    free_up_memory()

    with torch.no_grad():
        for idx, piece in enumerate(slices):
            skip_start = idx != 0
            skip_end = idx != len(slices) - 1
            audio = infer_multilang(
                piece,
                reference_audio=reference_audio,
                emotion=emotion,
                sdp_ratio=sdp_ratio,
                noise_scale=noise_scale,
                noise_scale_w=noise_scale_w,
                length_scale=length_scale,
                sid=speaker,
                language=language[idx],
                hps=hps,
                net_g=net_g,
                device=device,
                skip_start=skip_start,
                skip_end=skip_end,
            )
            audio16bit = gr.processing_utils.convert_to_16_bit_wav(audio)
            audio_list.append(audio16bit)
    return audio_list


def tts_split(
    text: str,
    speaker,
    sdp_ratio,
    noise_scale,
    noise_scale_w,
    length_scale,
    language,
    cut_by_sent,
    interval_between_para,
    interval_between_sent,
    reference_audio,
    emotion,
    style_text,
    style_weight,
):
    while text.find("\n\n") != -1:
        text = text.replace("\n\n", "\n")
    text = text.replace("|", "")
    para_list = re_matching.cut_para(text)
    para_list = [p for p in para_list if p != ""]
    audio_list = []
    for p in para_list:
        if not cut_by_sent:
            audio_list += process_text(
                p,
                speaker,
                sdp_ratio,
                noise_scale,
                noise_scale_w,
                length_scale,
                language,
                reference_audio,
                emotion,
                style_text,
                style_weight,
            )
            silence = np.zeros((int)(44100 * interval_between_para), dtype=np.int16)
            audio_list.append(silence)
        else:
            audio_list_sent = []
            sent_list = re_matching.cut_sent(p)
            sent_list = [s for s in sent_list if s != ""]
            for s in sent_list:
                audio_list_sent += process_text(
                    s,
                    speaker,
                    sdp_ratio,
                    noise_scale,
                    noise_scale_w,
                    length_scale,
                    language,
                    reference_audio,
                    emotion,
                    style_text,
                    style_weight,
                )
                silence = np.zeros((int)(44100 * interval_between_sent))
                audio_list_sent.append(silence)
            if (interval_between_para - interval_between_sent) > 0:
                silence = np.zeros(
                    (int)(44100 * (interval_between_para - interval_between_sent))
                )
                audio_list_sent.append(silence)
            audio16bit = gr.processing_utils.convert_to_16_bit_wav(
                np.concatenate(audio_list_sent)
            )  # 对完整句子做音量归一
            audio_list.append(audio16bit)
    audio_concat = np.concatenate(audio_list)
    return ("Success", (hps.data.sampling_rate, audio_concat))


def process_mix(slice):
    _speaker = slice.pop()
    _text, _lang = [], []
    for lang, content in slice:
        content = content.split("|")
        content = [part for part in content if part != ""]
        if len(content) == 0:
            continue
        if len(_text) == 0:
            _text = [[part] for part in content]
            _lang = [[lang] for part in content]
        else:
            _text[-1].append(content[0])
            _lang[-1].append(lang)
            if len(content) > 1:
                _text += [[part] for part in content[1:]]
                _lang += [[lang] for part in content[1:]]
    return _text, _lang, _speaker


def process_auto(text):
    _text, _lang = [], []
    for slice in text.split("|"):
        if slice == "":
            continue
        temp_text, temp_lang = [], []
        sentences_list = split_by_language(slice, target_languages=["zh", "ja", "en"])
        for sentence, lang in sentences_list:
            if sentence == "":
                continue
            temp_text.append(sentence)
            if lang == "ja":
                lang = "jp"
            temp_lang.append(lang.upper())
        _text.append(temp_text)
        _lang.append(temp_lang)
    return _text, _lang


def process_text(
    text: str,
    speaker,
    sdp_ratio,
    noise_scale,
    noise_scale_w,
    length_scale,
    language,
    reference_audio,
    emotion,
    style_text=None,
    style_weight=0,
):
    audio_list = []
    if language == "mix":
        bool_valid, str_valid = re_matching.validate_text(text)
        if not bool_valid:
            return str_valid, (
                hps.data.sampling_rate,
                np.concatenate([np.zeros(hps.data.sampling_rate // 2)]),
            )
        for slice in re_matching.text_matching(text):
            _text, _lang, _speaker = process_mix(slice)
            if _speaker is None:
                continue
            print(f"Text: {_text}\nLang: {_lang}")
            audio_list.extend(
                generate_audio_multilang(
                    _text,
                    sdp_ratio,
                    noise_scale,
                    noise_scale_w,
                    length_scale,
                    _speaker,
                    _lang,
                    reference_audio,
                    emotion,
                )
            )
    elif language.lower() == "auto":
        _text, _lang = process_auto(text)
        print(f"Text: {_text}\nLang: {_lang}")
        audio_list.extend(
            generate_audio_multilang(
                _text,
                sdp_ratio,
                noise_scale,
                noise_scale_w,
                length_scale,
                speaker,
                _lang,
                reference_audio,
                emotion,
            )
        )
    else:
        audio_list.extend(
            generate_audio(
                text.split("|"),
                sdp_ratio,
                noise_scale,
                noise_scale_w,
                length_scale,
                speaker,
                language,
                reference_audio,
                emotion,
                style_text,
                style_weight,
            )
        )
    return audio_list


def tts_fn(
    text: str,
    speaker,
    sdp_ratio,
    noise_scale,
    noise_scale_w,
    length_scale,
    language,
    reference_audio,
    emotion,
    prompt_mode,
    style_text=None,
    style_weight=0,
):
    if style_text == "":
        style_text = None
    if prompt_mode == "Audio prompt":
        if reference_audio == None:
            return ("Invalid audio prompt", None)
        else:
            reference_audio = load_audio(reference_audio)[1]
    else:
        reference_audio = None

    audio_list = process_text(
        text,
        speaker,
        sdp_ratio,
        noise_scale,
        noise_scale_w,
        length_scale,
        language,
        reference_audio,
        emotion,
        style_text,
        style_weight,
    )

    audio_concat = np.concatenate(audio_list)
    return "Success", (hps.data.sampling_rate, audio_concat)


def format_utils(text, speaker):
    _text, _lang = process_auto(text)
    res = f"[{speaker}]"
    for lang_s, content_s in zip(_lang, _text):
        for lang, content in zip(lang_s, content_s):
            res += f"<{lang.lower()}>{content}"
        res += "|"
    return "mix", res[:-1]


def load_audio(path):
    audio, sr = librosa.load(path, 48000)
    # audio = librosa.resample(audio, 44100, 48000)
    return sr, audio


def gr_util(item):
    if item == "Text prompt":
        return {"visible": True, "__type__": "update"}, {
            "visible": False,
            "__type__": "update",
        }
    else:
        return {"visible": False, "__type__": "update"}, {
            "visible": True,
            "__type__": "update",
        }

from flask import Flask, request, Response
from scipy.io import wavfile
from io import BytesIO
import time

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

current_config = DEFAULT_CONFIG
hps = None

def tts_init():
    global config
    global net_g
    global hps

    global model_path
    global config_path
    global speaker_name

    config = Config(current_config)
    model_path = config.webui_config.model
    config_path = config.webui_config.config_path
    speaker_name = None

    hps = utils.get_hparams_from_file(config_path)
    # 若config.json中未指定版本则默认为最新版本
    version = hps.version if hasattr(hps, "version") else latest_version
    print(model_path)
    net_g = get_net_g(
        model_path=model_path, version=version, device=device, hps=hps
    )
    speaker_ids = hps.data.spk2id
    speakers = list(speaker_ids.keys())
    languages = ["ZH", "JP", "EN", "auto", "mix"]

    if speaker_name is None:
        speaker_name = speakers[0]
        print("Speaker: ", speaker_name)

    try:
        speaker = speaker_name
        sdp_ratio = 0.5
        noise_scale = 0.6
        noise_scale_w = 0.9
        length_scale = 1
        prompt_mode = "Text prompt"
        text_prompt = "Happy"
        emotion = text_prompt
        reference_audio = ""
    except:
        print("Invalid Parameter")

    try:
        result, audio = tts_fn(
            "Hello",
            speaker,
            sdp_ratio,
            noise_scale,
            noise_scale_w,
            length_scale,
            'EN',
            reference_audio,
            emotion,
            prompt_mode,
            style_text=None,
            style_weight=0,
        )
        result, audio = tts_fn(
            "こんいちわ",
            speaker,
            sdp_ratio,
            noise_scale,
            noise_scale_w,
            length_scale,
            'JP',
            reference_audio,
            emotion,
            prompt_mode,
            style_text=None,
            style_weight=0,
        )
        result, audio = tts_fn(
            "大家好",
            speaker,
            sdp_ratio,
            noise_scale,
            noise_scale_w,
            length_scale,
            'ZH',
            reference_audio,
            emotion,
            prompt_mode,
            style_text=None,
            style_weight=0,
        )
    except:
        print("TTS Error")
        return "TTS Error"
    return "Success"

@app.route("/tts", methods=['POST'])
def main():
    print("Request received")
    start = time.time()
    data = request.json
    text = data['text']
    text_language = data['text_language']
    language = "auto"
    try:
        speaker = speaker_name
        sdp_ratio = 0.5
        noise_scale = 0.6
        noise_scale_w = 0.9
        length_scale = 1
        prompt_mode = "Text prompt"
        text_prompt = "Happy"
        emotion = text_prompt
        reference_audio = ""
    except:
        return "Invalid Parameter"

    try:
        result, audio = tts_fn(
            text,
            speaker,
            sdp_ratio,
            noise_scale,
            noise_scale_w,
            length_scale,
            language,
            reference_audio,
            emotion,
            prompt_mode,
            style_text=None,
            style_weight=0,
        )
        print("finished")
        with BytesIO() as wav:
            wavfile.write(wav, audio[0], audio[1])
            torch.cuda.empty_cache()
            print("Time:", time.time() - start)
            return Response(wav.getvalue(), mimetype="audio/wav")
    except Exception as e:
        print(e)
        return "TTS Error"

@app.route("/change_model", methods=['POST'])
def change_model():
    global current_config

    data = request.json
    new_config = DEFAULT_CONFIG
    if 'config' in data and data['config'] != "":
        new_config = "config_" + data['config'] + ".yml"
    
    if new_config == current_config:
        print("Model not changed")
        return f"Model not changed {current_config}"
    else:
        if not os.path.isfile(new_config):
            print("Model not found")
            return f"Model not found {current_config}"
        current_config = new_config
        tts_init()

    return f"Model Setup Success {current_config}"

if __name__ == "__main__":
    
    if config.webui_config.debug:
        logger.info("Enable DEBUG-LEVEL log")
        logging.basicConfig(level=logging.DEBUG)

    model_path = config.webui_config.model
    config_path = config.webui_config.config_path
    speaker_name = None
    
    tts_init()

    app.run(debug=False, host='0.0.0.0', port=9880)