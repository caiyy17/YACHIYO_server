class StreamCutter:
    def __init__(self, config):
        self.config = config

        self.punctuations = set(
            self.config.get("punctuations", "、：，。？！:,.?!\n\t")
        )
        self.length_threshold = self.config.get("sentence_length_threshold", 10)
        self.extra_info = self.config.get("extra_info", {})
        self.reset()

    def reset(self):
        self.stream_mode = "default"
        self.current_sentence = {}

    def calculate_effective_length(self, text):
        length = 0
        for char in text:
            if char not in self.punctuations and char != " ":
                length += 1
        return length

    def cut(self, text):
        output = []
        for i, char in enumerate(text):
            if self.stream_mode != "default":
                if char in self.extra_info[self.stream_mode]["end_mark"]:
                    self.stream_mode = "default"
                    self.current_sentence["raw_text"] = (
                        self.current_sentence.get("raw_text", "") + char
                    )
                else:
                    self.current_sentence[self.stream_mode] = (
                        self.current_sentence.get(self.stream_mode, "") + char
                    )
                    self.current_sentence["raw_text"] = (
                        self.current_sentence.get("raw_text", "") + char
                    )
            else:
                for mode in self.extra_info:
                    if char in self.extra_info[mode]["start_mark"]:
                        if (
                            self.current_sentence.get(mode, "").strip() != ""
                            or self.current_sentence.get("text", "").strip() != ""
                        ):
                            output.append(self.current_sentence)
                        self.stream_mode = mode
                        self.current_sentence = {}
                        self.current_sentence["raw_text"] = char
                        break
                else:
                    self.current_sentence["text"] = (
                        self.current_sentence.get("text", "") + char
                    )
                    self.current_sentence["raw_text"] = (
                        self.current_sentence.get("raw_text", "") + char
                    )
                    if (
                        char in self.punctuations
                        and self.calculate_effective_length(
                            self.current_sentence.get("text", "")
                        )
                        > self.length_threshold
                    ):
                        output.append(self.current_sentence)
                        self.current_sentence = {}
        return output

    def cut_last(self):
        output = []
        if self.current_sentence.get("raw_text", "").strip() != "":
            output.append(self.current_sentence)
            self.stream_mode = "default"
            self.current_sentence = {}
        return output
