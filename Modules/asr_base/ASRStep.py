import base64
from ..base.BaseProcessingStep import BaseProcessingStep


class BaseASRCaller:
    def __init__(self):
        pass

    def call(self, audio_file):
        text = "This is an ASR test. "
        lang = "en"
        return text, lang


class ASRStep(BaseProcessingStep):
    def custom_init(self):
        self.asr_caller = BaseASRCaller()

    def process(self, data, pass_data={}):
        audio_file = base64.b64decode(data.get("audio_file", ""))
        asr_result, lang = self.asr_caller.call(audio_file)
        output_data = {}
        self.add_output(output_data, "result", asr_result)
        self.add_output(output_data, "language", lang)
        self.output_to_queue(output_data, pass_data)
        return
