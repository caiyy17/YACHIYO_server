import re
from langdetect import detect

def detect_language(prompt, preferred_language="en"):
    # return "zh"
    try:
        if len(prompt) == 0:
            return preferred_language
        language = detect(prompt)
        if language[:2] == "zh" or language in ["ja", "ko"]:
            language = "zh"
        elif language == "en":
            language = "en"
        else:
            print("Unknown Language: ", language, ". Assuming ", preferred_language, ". Prompt: ", prompt)
        return language
    except Exception as e:
        print("Error in detecting language: ", e, ". Prompt: ", prompt)
        return preferred_language

def cut_prompt(prompt, preferred_language="en", length_threshold = 8, limit = 200):
    prompts = []
    mark = ["，", ".", "!", "?", ",", "。", "！", "？",";", "；", ":", "：", "…", "、", "\n", "\t", "\r"]
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
    
def cut_prompt_motion(prompt, preferred_language="en", length_threshold = 8, limit = 200):
    prompts = []
    mark_start = "["
    mark_end = "]"
    last_cut = 0

    has_motion = False

    for i, char in enumerate(prompt):
        if char == mark_start:
            if i > last_cut:
                has_motion = True
                prompts.append(prompt[last_cut:i])
                last_cut = i
    if last_cut < len(prompt):
        prompts.append(prompt[last_cut:])
    
    out_prompts = []
    if has_motion:
        for i in range(len(prompts)):
            # 如果有mark_end，提取开始到mark_end的内容
            if mark_end in prompts[i]:
                motion = prompts[i].split(mark_end)[0] + mark_end
                prompts[i] = prompts[i].split(mark_end)[1]
                out_prompts.append([motion, "motion"])
                out_prompts.append(prompt in cut_prompt(prompts[i], preferred_language, length_threshold, limit))
            else:
                if i == len(prompts) - 1:
                    out_prompts.append(prompts[i])
                else:
                    out_prompts.append(prompt in cut_prompt(prompts[i], preferred_language, length_threshold, limit))
    else:
        out_prompts.append(prompt in cut_prompt(prompt, preferred_language, length_threshold, limit))
        
    return out_prompts

def get_text(prompt):
    prompt = re.sub(r"[^\u3040-\u30ff\u31f0-\u31ff\u4e00-\u9fa5a-zA-Z0-9\s]", "", prompt)
    prompt = re.sub(r"[\n\t\r]", "", prompt)
    prompt = re.sub(r"\s+", " ", prompt)
    prompt = prompt.strip()
    return prompt