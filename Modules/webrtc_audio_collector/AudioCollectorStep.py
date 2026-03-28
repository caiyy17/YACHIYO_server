import base64
import io
import wave
import numpy as np

from ..base.SpanProcessingStep import SpanProcessingStep

SAMPLE_RATE = 48000  # WebRTC default sample rate


class AudioCollectorStep(SpanProcessingStep):
    """
    Collects individual audio frames between vad_start and vad_end signals,
    assembles them into a complete WAV file, and outputs to the next module (ASR).

    Inherits SpanProcessingStep: span = vad_start → vad_end.
    Cancel during span clears the audio buffer.

    Standard input format (from WebRTC server):
      - {"signal": "vad_start", "timestamp": ...}  -> start span
      - {"audio": "<b64_pcm>" or ["<pcm1>", ...], "timestamp": ...}  -> buffer audio
      - {"signal": "vad_end", "timestamp": ...}  -> assemble WAV and output

    Output:
      - {"audio_file": "<base64 WAV>", "timestamp": ...}
    """

    def span_init(self):
        self.catch_signal_set = {"vad_start", "vad_end"}
        self.sample_rate = self.get_config("sample_rate", SAMPLE_RATE)
        self.audio_buffer = []

    def span_process(self, data, pass_data={}):
        signal = data.get("signal", "")

        if signal == "vad_start":
            self.logger.info("VAD start - begin buffering")
            self.audio_buffer = []
            self.start_span(data["timestamp"])
            return

        if signal == "vad_end":
            n_frames = len(self.audio_buffer)
            total_samples = sum(len(f) for f in self.audio_buffer)
            duration = total_samples / self.sample_rate
            self.logger.info(
                f"VAD end - assembling {n_frames} frames ({duration:.2f}s)"
            )
            if self.audio_buffer:
                wav_b64 = self._assemble_wav()
                output_data = {}
                self.add_output(output_data, "audio_file", wav_b64)
                # Use span timestamp (vad_start) for output
                self.output_to_queue(output_data, {"timestamp": self.current_timestamp})
            self.audio_buffer = []
            self.end_span()
            return

        # Regular audio frame(s) — buffer during span
        if not self.span_active:
            return

        audio_data = data.get("audio", "")
        if audio_data:
            if isinstance(audio_data, list):
                for a_b64 in audio_data:
                    pcm = np.frombuffer(base64.b64decode(a_b64), dtype=np.int16)
                    self.audio_buffer.append(pcm)
            else:
                pcm = np.frombuffer(base64.b64decode(audio_data), dtype=np.int16)
                self.audio_buffer.append(pcm)

    def on_span_cancel(self, cancel_message):
        self.audio_buffer = []
        self.logger.info("Cancel - cleared audio buffer")

    def _assemble_wav(self):
        """Assemble buffered PCM frames into a base64-encoded WAV."""
        pcm = np.concatenate(self.audio_buffer)
        buf = io.BytesIO()
        with wave.open(buf, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(self.sample_rate)
            wf.writeframes(pcm.tobytes())
        return base64.b64encode(buf.getvalue()).decode("ascii")
