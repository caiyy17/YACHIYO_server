import re
from langdetect import detect

def detect_language(prompt, preferred_language="en"):
    # return "zh"
    try:
        language = detect(prompt)
        if language[:2] == "zh" or language in ["ja", "ko"]:
            language = "zh"
        elif language == "en":
            language = "en"
        else:
            print("Unknown Language: ", language, ". Assuming ", preferred_language)
        return language
    except Exception as e:
        print("Error in detecting language: ", e)
        return preferred_language

def cut_prompt(prompt, preferred_language="en", length_threshold = 8, limit = 200):
    prompts = []
    mark = ["，", ".", "!", "?", ",", "。", "！", "？",";", "；", ":", "：", "…", "、"]
    last_cut = 0
    for i, char in enumerate(prompt):
        # 检查是否达到分割条件：标点符号或最大长度
        if char in mark and i - last_cut >= length_threshold or i - last_cut >= limit:
            # 发送当前段落到输出队列
            p = prompt[last_cut:i + 1]
            # remove all the special characters
            text_part = get_text(p)
            language = detect_language(text_part, preferred_language)
            if language == "en":
                p = p + " "
            print("p: " + p + " language: " + language)
            prompts.append([p, language])
            last_cut = i + 1  # 更新上一个分割点的位置
    # 处理最后一段
    prompt = prompt[last_cut:]
    text_part = get_text(prompt)
    language = detect_language(text_part, preferred_language)
    # print("prompt: " + prompt + " language: " + language)
    prompts.append([prompt, language])
    return prompts
    

def get_text(prompt):
    prompt = re.sub(r"[^\u3040-\u30ff\u31f0-\u31ff\u4e00-\u9fa5a-zA-Z0-9\s]", "", prompt)
    prompt = re.sub(r"[\n\t\r]", "", prompt)
    prompt = re.sub(r"\s+", " ", prompt)
    prompt = prompt.strip()
    return prompt