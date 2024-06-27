import re
from langdetect import detect

def get_processed(accumulated_text, language):
    processed = language_process(accumulated_text, language)
    # 保留最后一句，并且将最后一句的标点符号去掉
    accumulated_text = processed[-1]
    processed = processed[:-1]
    return processed, accumulated_text

def cut_prompt(prompt, length, mark):
    # 对于mark中的每一个符号，将prompt切分成多个句子
    escaped_punctuations = [re.escape(p) for p in mark]
    pattern = '|'.join(escaped_punctuations)
    prompt = re.split(pattern, prompt)
    # 挑选其中不为空的句子
    prompt = [p.strip() for p in prompt]
    prompt = [p + mark[0] for p in prompt if p != '']
    new_prompt = []
    new_p = prompt[0]
    for i in range(1, len(prompt)):
        if len(new_p + prompt[i]) < length:
            new_p += prompt[i]
        else:
            new_prompt.append(new_p)
            new_p = prompt[i]
    new_prompt.append(new_p)
    if mark[0] == ' ':
        new_prompt = [p[:-1] + '. ' for p in new_prompt]
    return new_prompt

def english_process(prompt, length=100):
    # 去除回车
    prompt = prompt + ' '
    # 切分长文本，在句号后面切分
    prompt = cut_prompt(prompt, length, ['. ', '! ', '? '])
    # 对于每个长于length的句子，再次切分
    new_prompt = []
    for i in range(len(prompt)):
        if len(prompt[i]) > length:
            # 去掉末尾的标点符号
            sign = prompt[i][-2:]
            prompt[i] = prompt[i][:-2]
            prompt[i] = cut_prompt(prompt[i], length, [', '])
            prompt[i][-1] = prompt[i][-1][:-2] + sign
            new_prompt += prompt[i]
        else:
            new_prompt.append(prompt[i])
    prompt = new_prompt
    # 最后对于每个长于length的字段，再次切分
    # new_prompt = []
    # for i in range(len(prompt)):
    #     if len(prompt[i]) > length:
    #         # 去掉末尾的标点符号
    #         sign = prompt[i][-2:]
    #         prompt[i] = prompt[i][:-2]
    #         prompt[i] = cut_prompt(prompt[i], length, [' '])
    #         prompt[i][-1] = prompt[i][-1][:-2] + sign
    #         new_prompt += prompt[i]
    #     else:
    #         new_prompt.append(prompt[i])
    return new_prompt

def chinese_process(prompt, length=30):
    # 去除回车
    prompt = prompt.replace(' ', '')
    # 切分长文本，在句号后面切分
    prompt = cut_prompt(prompt, length, ['。', '！', '？'])
    # 对于每个长于30的句子，再次切分
    new_prompt = []
    for i in range(len(prompt)):
        if len(prompt[i]) > length:
            # 去掉末尾的标点符号
            sign = prompt[i][-1]
            prompt[i] = prompt[i][:-1]
            prompt[i] = cut_prompt(prompt[i], length, ['，'])
            prompt[i][-1] = prompt[i][-1][:-1] + sign
            new_prompt += prompt[i]
        else:
            new_prompt.append(prompt[i])
    return new_prompt

def detect_language(prompt):
    # return "zh"
    language = detect(prompt)
    if language[:2] == "zh" or language in ["ja", "ko"]:
        language = "zh"
        # print("Language: ", language)
    elif language == "en":
        language = "en"
        # print("Language: ", language)
    else:
        print("Unknown Language: ", language, "Assuming English")
        language = "en"
    return language

def language_process(prompt, language):
    prompt = prompt.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('*', '').replace('`', '')
    if language == "zh":
        new_prompt = chinese_process(prompt)
    elif language == "en":
        new_prompt = english_process(prompt)
    else:
        new_prompt = english_process(prompt)
    print("Processed Prompt: ", new_prompt)
    return new_prompt