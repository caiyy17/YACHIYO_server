import requests
from .config import *

class TestLLMProcessMotionCaller:
    def __init__(self):
        pass
    def call(self, prompt):
        output = []
        motion = "motion info. "
        text = prompt
        output.append([text, motion])
        return output
    def call_last(self):
        output = []
        motion = "motion info. "
        text = "last motion"
        output.append([text, motion])
        return output
    def reset(self):
        pass
    
class GPT2MotionCaller:
    def __init__(self):
        self.start_mark = "["
        self.end_mark = "]"
        self.punctuations = set("：，。？！:,.?!\n\t")
        self.length_threshold = 3

        self.accumulated_text = ""
        self.current_sentence = ""
        self.extra_info = ""
        self.in_brackets = False

    def calculate_effective_length(self, text):
        length = 0
        for char in text:
            if char not in self.punctuations and char != " ":
                length += 1
        return length

    def call(self, prompt):
        output = []

        for i, char in enumerate(prompt):
            if self.in_brackets:  # 如果在括号内
                if char == self.end_mark:
                    self.in_brackets = False
                else:
                    self.extra_info += char
            else:
                if char == self.start_mark:
                    if self.current_sentence.strip() != "" or self.extra_info.strip() != "":
                        output.append([self.current_sentence, self.extra_info])
                    self.in_brackets = True  # 进入括号内
                    self.current_sentence = ""
                    self.extra_info = ""
                else:
                    self.current_sentence += char
                    if char in self.punctuations:
                        # 判断当前句子的长度是否大于5
                        if self.calculate_effective_length(self.current_sentence) > self.length_threshold:
                            # 输出分割的句子和括号内的额外信息
                            output.append([self.current_sentence, self.extra_info])
                            # 清空当前句子和额外信息
                            self.current_sentence = ""
                            self.extra_info = ""

        return output
    
    def call_last(self):
        output = []
        if self.current_sentence.strip() != "" or self.extra_info.strip() != "":
            output.append([self.current_sentence, self.extra_info])
            self.in_brackets = False
            self.current_sentence = ""
            self.extra_info = ""
        return output
    
    def reset(self):
        self.accumulated_text = ""
        self.current_sentence = ""
        self.extra_info = ""
        self.in_brackets = False