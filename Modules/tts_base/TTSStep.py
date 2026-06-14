from pydub import AudioSegment
from io import BytesIO

from ..base.BaseProcessingStep import BaseProcessingStep
from ..utils.functions import bytes_to_base64


class BaseTTSCaller:
    def __init__(self, logger):
        self.logger = logger
        self.repeat = 2
        self.empty_audio = AudioSegment.silent(duration=10)
        pass

    def call(self, prompt, language="auto", speaker=""):
        try:
            audio_data = self.empty_audio
            if prompt != "":
                for i in range(self.repeat):
                    audio = AudioSegment.from_file("test/test_voice.wav", format="wav")
                    audio_data += audio

            # Create a BytesIO object as an in-memory file
            audio_bytes_io = BytesIO()

            # Export audio data as WAV format into the BytesIO object
            audio_data.export(audio_bytes_io, format="wav")

            # Get the byte stream
            audio_bytes = audio_bytes_io.getvalue()
            return audio_bytes
        except Exception as e:
            self.logger.error(f"failed to call tts: {e}")
            return ""


class TTSStep(BaseProcessingStep):
    def init_timeline_config(self):
        self.emit_audio_timeline = self.config.get("emit_audio_timeline", False)
        self.audio_timeline_ms = 0.0
        self.gesture_timeline = []
        if self.emit_audio_timeline:
            self.catch_signal_set = {"SoS", "EoS"}

    def custom_init(self):
        self.init_timeline_config()
        self.tts_caller = BaseTTSCaller(self.logger)
        self.tts_caller.call("test")

    def reset_audio_timeline(self):
        self.audio_timeline_ms = 0.0
        self.gesture_timeline = []

    def get_audio_duration_ms(self, audio_bytes):
        if not audio_bytes:
            return 0
        try:
            audio = AudioSegment.from_file(BytesIO(audio_bytes), format="wav")
            return len(audio)
        except Exception as e:
            self.logger.error(f"failed to calculate audio duration: {e}")
            return 0

    def build_gesture_events(self, data, text, start_ms, duration_ms):
        gesture_plan = data.get("gesture_plan") or []
        if not isinstance(gesture_plan, list):
            return []
        try:
            sentence_index = int(data.get("sentence_index", -1))
        except (TypeError, ValueError):
            return []

        events = []
        for item in gesture_plan:
            if not isinstance(item, dict):
                continue
            try:
                item_sentence_index = int(item.get("sentence_index", -1))
                start_ratio = float(item.get("start_ratio", 0.0))
                end_ratio = float(item.get("end_ratio", 0.0))
            except (TypeError, ValueError):
                continue
            if item_sentence_index != sentence_index:
                continue
            start_ratio = max(0.0, min(1.0, start_ratio))
            end_ratio = max(0.0, min(1.0, end_ratio))
            if end_ratio <= start_ratio:
                continue
            event_start_ms = start_ms + duration_ms * start_ratio
            event_end_ms = start_ms + duration_ms * end_ratio
            event = {
                "action": item.get("action", ""),
                "label": item.get("label", item.get("action", "")),
                "sentence_index": sentence_index,
                "sentence_text": text,
                "start": round(event_start_ms / 1000.0, 3),
                "end": round(event_end_ms / 1000.0, 3),
                "duration": round(max(0.0, event_end_ms - event_start_ms) / 1000.0, 3),
                "start_ms": int(round(event_start_ms)),
                "end_ms": int(round(event_end_ms)),
                "source": {
                    "start_ratio": start_ratio,
                    "end_ratio": end_ratio,
                },
            }
            events.append(event)
        return events

    def add_timeline_outputs(self, output_data, data, text, duration_ms):
        start_ms = self.audio_timeline_ms
        end_ms = start_ms + duration_ms

        self.add_output(output_data, "audio_start_ms", int(round(start_ms)))
        self.add_output(output_data, "audio_end_ms", int(round(end_ms)))
        self.add_output(output_data, "audio_duration_ms", int(round(duration_ms)))

        events = self.build_gesture_events(data, text, start_ms, duration_ms)
        self.gesture_timeline.extend(events)
        self.add_output(output_data, "gesture_events", events)

        self.audio_timeline_ms = end_ms

    def process(self, data, pass_data={}):
        signal = data.get("signal", "")
        if signal == "SoS":
            if self.emit_audio_timeline:
                self.reset_audio_timeline()
            self.output_to_queue({"signal": "SoS"}, pass_data)
            return
        if signal == "EoS":
            output_data = {"signal": "EoS"}
            if self.emit_audio_timeline:
                self.add_output(
                    output_data,
                    "audio_total_ms",
                    int(round(self.audio_timeline_ms)),
                )
                self.add_output(
                    output_data,
                    "gesture_timeline",
                    self.gesture_timeline,
                )
            self.output_to_queue(output_data, pass_data)
            return

        text = data.get("text", "")
        language = data.get("language", "auto")
        speaker = data.get("speaker", "")
        text = text.strip("\n")
        tts_result = self.tts_caller.call(text, language, speaker)
        audio_duration_ms = (
            self.get_audio_duration_ms(tts_result) if self.emit_audio_timeline else 0
        )
        try:
            tts_result_b64 = bytes_to_base64(tts_result)
        except Exception as e:
            self.logger.error(f"failed to convert tts_result to base64: {e}")
            tts_result_b64 = ""
        # Put data into output_queue
        output_data = {}
        self.add_output(output_data, "audio_file", tts_result_b64)
        if self.emit_audio_timeline:
            self.add_timeline_outputs(output_data, data, text, audio_duration_ms)
        self.output_to_queue(output_data, pass_data)
        return
