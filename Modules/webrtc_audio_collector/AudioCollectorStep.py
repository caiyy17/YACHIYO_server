import base64
import io
import wave
import numpy as np

from ..base.SpanProcessingStep import SpanProcessingStep

SAMPLE_RATE = 48000  # WebRTC default sample rate


class AudioCollectorStep(SpanProcessingStep):
    """
    Collects individual audio frames between recording_start and recording_end
    signals, assembles them into a complete WAV file, and outputs to the next module (ASR).

    Inherits SpanProcessingStep: span = recording_start → recording_end.
    Cancel during span clears the audio buffer.

    Standard input format (from WebRTC server):
      - {"signal": "recording_start", "timestamp": ...}  -> start span
      - {"audio": "<b64_pcm>" or ["<pcm1>", ...], "timestamp": ...}  -> buffer audio
      - {"signal": "recording_end", "timestamp": ...}  -> assemble WAV and output

    Output:
      - {"audio_file": "<base64 WAV>", "timestamp": ...}
    """

    def span_init(self):
        self.catch_signal_set = {"recording_start", "recording_end"}
        self.sample_rate = self.get_config("sample_rate", SAMPLE_RATE)
        self.audio_buffer = []

    def span_process(self, data, pass_data={}):
        signal = data.get("signal", "")

        if signal == "recording_start":
            self.logger.info("recording_start - begin buffering")
            self.audio_buffer = []
            self.start_span(data["timestamp"])
            # Caught here to delimit the audio span; re-emit it so it continues
            # through the pipeline and is dispatched back to the client via
            # DataChannel (WebRTC) in pipeline order.
            self.output_to_queue(
                {"signal": "recording_start"}, {"timestamp": data["timestamp"]}
            )
            return

        if signal == "recording_end":
            n_frames = len(self.audio_buffer)
            total_samples = sum(len(f) for f in self.audio_buffer)
            duration = total_samples / self.sample_rate
            self.logger.info(
                f"recording_end - assembling {n_frames} frames ({duration:.2f}s)"
            )
            # Re-emit recording_end before the WAV so it forwards ahead of ASR
            # processing and reaches the client via DataChannel before the response.
            # Same timestamp as the WAV (span start) so cancel treats them as one turn.
            if self.span_active:
                self.output_to_queue(
                    {"signal": "recording_end"}, {"timestamp": self.current_timestamp}
                )
            if self.audio_buffer:
                wav_b64 = self._assemble_wav()
                output_data = {}
                self.add_output(output_data, "audio_file", wav_b64)
                # Use span timestamp (recording_start) for output
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
