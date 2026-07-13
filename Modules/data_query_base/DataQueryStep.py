from ..base.BaseProcessingStep import BaseProcessingStep


class BaseDataQueryCaller:
    def __init__(self):
        pass

    def call(self, prompt):
        result = prompt
        return result


class DataQueryStep(BaseProcessingStep):
    REQUIRED_INPUTS = ["prompt"]
    OUTPUTS = ["result"]

    def custom_init(self):
        self.data_query_caller = BaseDataQueryCaller()

    def process(self, data, pass_data={}):
        prompt = data.get("prompt", "")
        if prompt == "":
            result = ""
        else:
            result = self.data_query_caller.call(prompt)
        # Put data into output_queue
        output_data = {}
        self.add_output(output_data, "result", result)
        self.output_to_queue(output_data, pass_data)
        return
