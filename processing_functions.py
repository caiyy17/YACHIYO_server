import time
import queue
import threading

TIMEOUT=1
MESSAGE_MAX_LENGTH = 200

class BaseProcessingStep:
    def __init__(self, 
                 index,
                 client_id, 
                 logger, 
                 send_queue, 
                 input_queue, 
                 output_queue, 
                 cancel_queue, 
                 kill_event, 
                 config=None
                 ):
        self.name = self.__class__.__name__
        self.index = index
        self.client_id = client_id
        self.logger = logger
        self.send_queue = send_queue
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.cancel_queue = cancel_queue
        self.cancel_timestamp = 0
        self.kill_event = kill_event
        self.config = config or {}  # 可变的设置信息，默认是空字典
        self.reserved_input_vars = ["destination", "signal", "timestamp", "timestamp_remote"]
        self.output_dict = {}
        self.catch_signal_set = set()
        self.prepare_output_dict()
        self.custom_init()
        logger.info(f"{self.name}: initialized")

    def custom_init(self):
        """子类可以重写此方法进行自定义初始化"""
        pass

    def log_info(self, message, cut=True):
        if cut and len(message) > MESSAGE_MAX_LENGTH:
            message = message[:MESSAGE_MAX_LENGTH] + "..."
        self.logger.info(f"{self.name}: {message}")

    def log_error(self, message, cut=True):
        if cut and len(message) > MESSAGE_MAX_LENGTH:
            message = message[:MESSAGE_MAX_LENGTH] + "..."
        self.logger.error(f"{self.name}: {message}")

    def run(self):
        while True:
            if self.kill_event.is_set():
                break
            self.check_cancel()
            try:
                data = self.input_queue.get(timeout=TIMEOUT)
                data = json.loads(data)
                if data.get("timestamp", 0) < self.cancel_timestamp:
                    self.log_info(f"discarding old data: {data}")
                    self.custom_cancel()
                    continue
                if data.get("destination", self.index) == self.index:
                    # 如果这是一个signal，并且不在catch_signal_set中，就除掉destination，直接送到output_queue
                    if data.get("signal", None) is not None and data["signal"] not in self.catch_signal_set:
                        data.pop("destination", None)
                        self.output_queue.put(json.dumps(data))
                        continue

                    filtered_data = self.extract_input_data(data)
                    pass_data = self.extract_pass_data(data)
                    self.process(filtered_data, pass_data)
                else:
                    self.output_queue.put(json.dumps(data))
            except queue.Empty:
                pass
            except Exception as e:
                self.log_error(f"{e}")

    def check_cancel(self):
        if not self.cancel_queue.empty():
            while not self.cancel_queue.empty():
                cancel = self.cancel_queue.get()
                cancel = json.loads(cancel)
                self.log_info(f"received cancel signal: {cancel}")
                self.cancel_timestamp = cancel["timestamp"]

    def custom_cancel(self):
        """子类可以重写此方法进行自定义取消操作"""
        pass

    def extract_input_data(self, data):
        """
        从输入数据中提取需要的键值对。
        根据配置中的 input_vars，从 data 中提取指定的变量。
        :param data: 上一个节点的输出（字典结构）
        :return: 提取的输入数据字典
        """
        extracted_data = {}

        if "signal" in data:
            extracted_data["signal"] = data["signal"]
        if "timestamp" in data:
            extracted_data["timestamp"] = data["timestamp"]

        input_vars = self.config.get("input_vars", [])
        for input_var in input_vars:
            input_name = input_var["input_name"]
            sources = input_var["sources"]
            
            # 在 previous_outputs 中依次查找 sources
            for source in sources:
                # 找到第一个匹配的来源，并存储到 extracted_data 中
                if source in data:
                    extracted_data[input_name] = data[source]
                    break  # 一旦找到匹配的来源就停止查找

        return extracted_data
    
    def extract_pass_data(self, data):
        """
        从输入数据中提取需要的键值对。
        根据配置中的 pass_vars，从 data
        中提取指定的变量。
        :param data: 上一个节点的输出（字典结构）
        :return: 提取的输入数据字典
        """
        extracted_data = {}

        if "timestamp" in data:
            extracted_data["timestamp"] = data["timestamp"]
        if "timestamp_remote" in data:
            extracted_data["timestamp_remote"] = data["timestamp_remote"]

        pass_vars = self.config.get("pass_vars", [])
        for pass_var in pass_vars:
            pass_name = pass_var["targets"][0]
            sources = pass_var["sources"]
            
            # 在 previous_outputs 中依次查找 sources
            for source in sources:
                # 找到第一个匹配的来源，并存储到 extracted_data 中
                if source in data:
                    extracted_data[pass_name] = data[source]
                    break

        return extracted_data
    
    def add_pass_data(self, data, pass_data):
        # 将 pass_data 中的数据添加到 data 中
        for key, value in pass_data.items():
            if key not in data:
                data[key] = value
        return data
    
    def add_destination(self, data, index=0):
        # 如果是 -1，就直接送到最后
        if index == -1:
            data["destination"] = -1
            return data
        
        destination = self.config.get("next_nodes", None)
        if destination is not None:
            if index < len(destination):
                data["destination"] = destination[index]
        return data
    
    def prepare_output_dict(self):
        output_vars = self.config.get("output_vars", [])
        self.output_dict = {}

        for output_var in output_vars:
            output_name = output_var["output_name"]
            self.output_dict[output_name] = output_var["targets"][0]

    def output_name(self, name):
        return self.output_dict.get(name, name)
    
    def output_to_queue(self, data, pass_data={}, 
                        *, 
                        is_add_timestamp=True, 
                        is_add_destination=True, 
                        destination_index=0, 
                        is_add_pass_data=True, 
                        is_log=True,
                        direct_send=False):
        if is_add_destination:
            self.add_destination(data, destination_index)
        if is_add_pass_data:
            self.add_pass_data(data, pass_data)
        elif is_add_timestamp:
            if "timestamp" in pass_data:
                data["timestamp"] = pass_data["timestamp"]
            if "timestamp_remote" in pass_data:
                data["timestamp_remote"] = pass_data["timestamp_remote"]
        
        if direct_send:
            if is_log:
                self.log_info(f"directly send data: {data}")
            self.send_queue.put(json.dumps(data))
        else:
            if is_log:
                self.log_info(f"output data: {data}")
            self.output_queue.put(json.dumps(data))
        return

    def process(self, data, pass_data={}):
        """根据提取的数据进行处理，子类可以重写此方法"""
        self.log_info(f"processing data: {data}")
        output_data = {self.output_name("result"): f"Processed by {self.name}"}
        self.output_to_queue(output_data)
        return

    def get_config(self, key, default=None):
        """从配置中获取特定的配置项"""
        return self.config.get(key, default)

class FuncA(BaseProcessingStep):
    def custom_init(self):
        self.sleep_time = self.get_config("sleep_time", 0)
    def process(self, data, pass_data={}):
        self.log_info(f"processing data: {data}")
        time.sleep(self.sleep_time)
        output_data = {self.output_name("output1"): "call_func_a"}
        self.output_to_queue(output_data, pass_data)
        return

class FuncB(BaseProcessingStep):
    def custom_init(self):
        self.sleep_time = self.get_config("sleep_time", 0)
    def process(self, data, pass_data={}):
        self.log_info(f"processing data: {data}")
        time.sleep(self.sleep_time)
        output_data = {self.output_name("output1"): "call_func_b"}
        self.output_to_queue(output_data, pass_data)
        return

import json
import base64

def bytes_to_base64(bytes_data):
    return base64.b64encode(bytes_data).decode('utf-8')

def base64_to_bytes(base64_data):
    return base64.b64decode(base64_data)

class ASRStep(BaseProcessingStep):
    def custom_init(self):
        from Modules.asr import TestASRCaller
        self.asr_caller = TestASRCaller()
    def process(self, data, pass_data={}):
        self.log_info(f"processing data: {data}")
        audio_file = base64.b64decode(data["audio_file"])
        asr_result = self.asr_caller.call(audio_file)
        output_data = {}
        output_data[self.output_name("result")] = asr_result
        output_data[self.output_name("language")] = "en"
        self.output_to_queue(output_data, pass_data)
        return
    
class SenseVoiceStep(ASRStep):
    def custom_init(self):
        from Modules.asr import SenceVoiceCaller
        self.asr_caller = SenceVoiceCaller()

class LLMStep(BaseProcessingStep):
    def custom_init(self):
        from Modules.llm import TestLLMCaller
        self.llm_caller = TestLLMCaller(self.get_config("sleep_time", 0))
        self.reset_history = self.get_config("reset_history", True)
        self.system_prompt = self.get_config("system_prompt", "")
        if self.reset_history:
            self.llm_caller.clear_history(self.client_id)
        if self.system_prompt != "":
            self.llm_caller.set_system_prompt(self.system_prompt, self.client_id)

    def process(self, data, pass_data={}):
        self.log_info(f"processing data: {data}")
        prompt = data["prompt"]
        for response in self.llm_caller.call_stream(prompt, self.client_id):
            self.check_cancel()
            if data["timestamp"] < self.cancel_timestamp:
                self.log_info(f"cancel inside loop")
                break
            current_data = {}
            current_data[self.output_name("text")] = response
            current_data[self.output_name("language")] = "en"
            self.output_to_queue(current_data, pass_data)
        eos_signal = {"signal": "EoS"}
        eos_signal[self.output_name("language")] = "en"
        self.output_to_queue(eos_signal, pass_data)
        return
    
class ChatgptStep(LLMStep):
    def custom_init(self):
        from Modules.llm import ChatgptCaller
        self.llm_caller = ChatgptCaller(self.get_config("sleep_time", 0))
        self.reset_history = self.get_config("reset_history", True)
        self.system_prompt = self.get_config("system_prompt", "")
        if self.reset_history:
            self.llm_caller.clear_history(self.client_id)
        if self.system_prompt != "":
            self.llm_caller.set_system_prompt(self.system_prompt, self.client_id)

class ChatGLMStep(LLMStep):
    def custom_init(self):
        from Modules.llm import ChatGLMCaller
        self.llm_caller = ChatGLMCaller(self.get_config("sleep_time", 0))
        self.reset_history = self.get_config("reset_history", True)
        self.system_prompt = self.get_config("system_prompt", "")
        if self.reset_history:
            self.llm_caller.clear_history(self.client_id)
        if self.system_prompt != "":
            self.llm_caller.set_system_prompt(self.system_prompt, self.client_id)

class TTSStep(BaseProcessingStep):
    def custom_init(self):
        from Modules.tts import TestTTSCaller
        self.tts_caller = TestTTSCaller()
        test_result = self.tts_caller.call("test", "en")
        
    def process(self, data, pass_data={}):
        self.log_info(f"processing data: {data}")
        text = data["text"]
        language = data["language"]
        text = text.strip("\n")
        tts_result = self.tts_caller.call(text, language)
        try:
            tts_result = bytes_to_base64(tts_result)
        except Exception as e:
            self.log_error(f"failed to convert tts_result to base64: {e}")
            tts_result = ""
        # 将数据放入 output_queue
        output_data = {}
        output_data[self.output_name("audio_file")] = tts_result
        self.output_to_queue(output_data, pass_data)
        return
    
class BertVitsStep(TTSStep):
    def custom_init(self):
        from Modules.tts import BertVitsCaller
        self.tts_caller = BertVitsCaller(self.get_config("voice", ""))
        self.log_info(self.tts_caller.voice_setup)
        test_result = self.tts_caller.call("test", "en")
    
class RAGStep(BaseProcessingStep):
    def custom_init(self):
        from Modules.rag import TestRAGCaller
        self.rag_caller = TestRAGCaller(self.get_config("rag_info", ""))
    def process(self, data, pass_data={}):
        self.log_info(f"processing data: {data}")
        prompt = data["prompt"]
        language = data["language"]
        if prompt == "":
            self.log_info("empty prompt")
            output_data = {}
            output_data[self.output_name("text")] = ""
            self.output_to_queue(output_data, pass_data)
        else:
            text, image = self.rag_caller.call(prompt, language)
            self.log_info(f"output data: {text}, {image}")
            # 将数据放入 output_queue
            send_data = {}
            send_data[self.output_name("image_send")] = image
            self.output_to_queue(send_data, pass_data, is_add_pass_data=False, destination_index=-1)
            output_data = {}
            output_data[self.output_name("text")] = text
            self.output_to_queue(output_data, pass_data)
        return
    
class MotionStep(BaseProcessingStep):
    def custom_init(self):
        from Modules.motion import TestMotionCaller
        self.motion_caller = TestMotionCaller()
    def process(self, data, pass_data={}):
        self.log_info(f"processing data: {data}")
        prompt = data["prompt"]
        if prompt == "":
            motion = ""
        else:
            motion = self.motion_caller.call(data["prompt"])
        # 将数据放入 output_queue
        output_data = {}
        output_data[self.output_name("motion")] = motion
        self.output_to_queue(output_data, pass_data)
        return
    
class BGEMotionStep(MotionStep):
    def custom_init(self):
        from Modules.motion import BGEMotionCaller
        self.motion_caller = BGEMotionCaller()
        init_response = self.motion_caller.init_dataset(self.get_config("dataset", ""))
        self.log_info(f"init_response: {init_response}")
    
class LLMProcessMotionStep(BaseProcessingStep):
    def custom_init(self):
        from Modules.llm_process_motion import TestLLMProcessMotionCaller
        self.llm_process_motion_caller = TestLLMProcessMotionCaller()
        self.catch_signal_set = {"EoS"}
    def custom_cancel(self):
        self.llm_process_motion_caller.reset()
    def process(self, data, pass_data={}):
        self.log_info(f"processing data: {data}")
        if data.get("signal", None) == "EoS":
            results = self.llm_process_motion_caller.call_last()
            for result in results:
                output_data = {}
                output_data[self.output_name("text")] = result[0]
                output_data[self.output_name("motion")] = result[1]
                self.output_to_queue(output_data, pass_data)
            eos_signal = {"signal": "EoS"}
            self.output_to_queue(eos_signal, pass_data, is_add_pass_data=False)
            return
        results = self.llm_process_motion_caller.call(data["text"])
        for result in results:
            output_data = {}
            output_data[self.output_name("text")] = result[0]
            output_data[self.output_name("motion")] = result[1]
            self.output_to_queue(output_data, pass_data)
        return
    
class GPT2MotionStep(LLMProcessMotionStep):
    def custom_init(self):
        from Modules.llm_process_motion import GPT2MotionCaller
        self.llm_process_motion_caller = GPT2MotionCaller()
        self.catch_signal_set = {"EoS"}

# 定义函数映射字典
FUNCTION_MAP = {
    'default': BaseProcessingStep,
    'call_func_a': FuncA,
    'call_func_b': FuncB,
    'call_asr': ASRStep,
    'call_sense_voice': SenseVoiceStep,
    'call_llm': LLMStep,
    'call_chatgpt': ChatgptStep,
    'call_chatglm': ChatGLMStep,
    'call_tts': TTSStep,
    'call_bert_vits': BertVitsStep,
    'call_rag': RAGStep,
    'call_motion': MotionStep,
    'call_bge_motion': BGEMotionStep,
    'call_llm_process_motion': LLMProcessMotionStep,
    'call_gpt_2_motion': GPT2MotionStep,
    # 'call_async_sender': AsyncSender,
}

def get_function_class_by_name(func_name):
    return FUNCTION_MAP.get(func_name, FUNCTION_MAP['default'])