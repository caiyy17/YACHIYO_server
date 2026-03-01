import base64
import io
import wave
import numpy as np

from ..base.BaseProcessingStep import BaseProcessingStep

SAMPLE_RATE = 48000  # WebRTC default sample rate


class AudioCollectorStep(BaseProcessingStep):
    """
    Collects individual audio frames between vad_start and vad_end signals,
    assembles them into a complete WAV file, and outputs to the next module (ASR).

    Standard input format (from WebRTC server):
      - {"signal": "vad_start", "timestamp": ...}  -> start buffering
      - {"audio": "<b64_pcm>" or ["<pcm1>", ...], "timestamp": ...}  -> buffer audio
      - {"signal": "vad_end", "timestamp": ...}  -> assemble WAV and output

    Output:
      - {"audio_file": "<base64 WAV>", "timestamp": ...}
    """

    def custom_init(self):
        self.catch_signal_set = {"vad_start", "vad_end"}
        self.sample_rate = self.get_config("sample_rate", SAMPLE_RATE)
        self.buffering = False
        self.audio_buffer = []  # list of PCM int16 numpy arrays
        self.vad_start_timestamp = None

    def process(self, data, pass_data={}):
        signal = data.get("signal", "")

        if signal == "vad_start":
            self.logger.info("VAD start - begin buffering")
            self.buffering = True
            self.audio_buffer = []
            self.vad_start_timestamp = data.get("timestamp", 0)
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
                # Use vad_start timestamp so cancel logic works correctly
                pd = dict(pass_data)
                if self.vad_start_timestamp:
                    pd["timestamp"] = self.vad_start_timestamp
                self.output_to_queue(output_data, pd)
            self.buffering = False
            self.audio_buffer = []
            self.vad_start_timestamp = None
            return

        # Regular audio frame(s) — single string or list of strings
        audio_data = data.get("audio", "")
        if audio_data and self.buffering:
            if isinstance(audio_data, list):
                for a_b64 in audio_data:
                    pcm = np.frombuffer(base64.b64decode(a_b64), dtype=np.int16)
                    self.audio_buffer.append(pcm)
            else:
                pcm = np.frombuffer(base64.b64decode(audio_data), dtype=np.int16)
                self.audio_buffer.append(pcm)
        return

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

    def custom_cancel(self, cancel_message):
        self.buffering = False
        self.audio_buffer = []
        self.vad_start_timestamp = None
        self.logger.info("Cancel - cleared audio buffer")
