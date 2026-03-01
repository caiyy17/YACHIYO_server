import base64
import io
import wave
from math import gcd

import numpy as np
from PIL import Image

from ..base.BaseProcessingStep import BaseProcessingStep

WEBRTC_SAMPLE_RATE = 48000
FRAME_SAMPLES = 960  # 20ms at 48kHz
VIDEO_FPS = 30
DATA_FPS = 20


class FrameSplitterStep(BaseProcessingStep):
    """
    Receives TTS audio output (base64 WAV), resamples to WebRTC rate,
    splits into synchronized audio-video-data groups, and outputs in standard format.

    Synchronization:
      Audio: 48kHz / 960 = 50fps (20ms per frame)
      Video: 30fps (~33.3ms per frame)
      Data:  20fps (text, motion, etc.)
      GCD(50, 30, 20) = 10 → 100ms per group
      Group: 5 audio + 3 video + 2 data = 100ms (atomic sync unit)

    Standard output format:
      {"audio": ["<pcm>"×5], "video": ["<jpeg>"×3], "data": [{...}, null]}
      data is a list of length data_per_group, each entry is dict or null.

    The WebRTC server consumes each group atomically:
      - 5 audio frames (100ms) → audio track buffer
      - 3 video frames (100ms) → video track buffer
      - 2 data frames (100ms) → DataChannel (non-null entries sent)
    """

    def custom_init(self):
        self.sample_rate = self.get_config("sample_rate", WEBRTC_SAMPLE_RATE)
        self.frame_samples = self.get_config("frame_samples", FRAME_SAMPLES)
        self.video_fps = self.get_config("video_fps", VIDEO_FPS)
        self.data_fps = self.get_config("data_fps", DATA_FPS)
        self.video_width = self.get_config("video_width", 320)
        self.video_height = self.get_config("video_height", 240)

        # Calculate sync group size from GCD of all frame rates
        audio_fps = self.sample_rate // self.frame_samples  # 50
        g = gcd(gcd(audio_fps, self.video_fps), self.data_fps)  # gcd(50,30,20) = 10
        self.audio_per_group = audio_fps // g          # 5
        self.video_per_group = self.video_fps // g     # 3
        self.data_per_group = self.data_fps // g       # 2
        group_ms = self.audio_per_group * self.frame_samples / self.sample_rate * 1000

        # Pre-generate white frame as base64 JPEG (reused for all groups)
        self._white_frame_b64 = self._make_jpeg_b64(255, 255, 255)
        self.logger.info(
            f"Sync group: {self.audio_per_group} audio + {self.video_per_group} video "
            f"+ {self.data_per_group} data ({group_ms:.0f}ms), "
            f"video {self.video_width}x{self.video_height}"
        )

    def _make_jpeg_b64(self, r, g, b):
        """Generate a solid color JPEG frame, base64 encoded."""
        img = Image.new("RGB", (self.video_width, self.video_height), (r, g, b))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return base64.b64encode(buf.getvalue()).decode("ascii")

    def process(self, data, pass_data={}):
        audio_data = data.get("audio_data", "")
        if not audio_data:
            return

        pcm_frames = self._decode_and_split(audio_data)
        if not pcm_frames:
            return

        # Build metadata for first group (forward all pass_data)
        meta = {k: v for k, v in pass_data.items() if v}

        # Group audio frames into sync groups
        group_count = 0
        for i in range(0, len(pcm_frames), self.audio_per_group):
            if self.check_cancel():
                self.logger.info("cancel inside loop")
                break
            group_audio = pcm_frames[i:i + self.audio_per_group]

            # Pad last group if incomplete
            while len(group_audio) < self.audio_per_group:
                group_audio.append(np.zeros(self.frame_samples, dtype=np.int16))

            audio_list = [
                base64.b64encode(f.tobytes()).decode("ascii")
                for f in group_audio
            ]
            video_list = [self._white_frame_b64] * self.video_per_group

            # Data list: metadata in first slot of first group, null elsewhere
            data_list = [None] * self.data_per_group
            if group_count == 0 and meta:
                data_list[0] = meta

            frame_data = {}
            self.add_output(frame_data, "audio", audio_list)
            self.add_output(frame_data, "video", video_list)
            self.add_output(frame_data, "data", data_list)

            self.output_to_queue(
                frame_data, pass_data,
                is_add_pass_data=False,
                is_add_timestamp=True,
                is_log=(group_count == 0),
            )
            group_count += 1

        duration = len(pcm_frames) * self.frame_samples / self.sample_rate
        self.logger.info(
            f"Split into {group_count} sync groups ({duration:.2f}s)"
        )

    def _decode_and_split(self, audio_b64):
        """Decode base64 WAV, resample to target rate, split into frames."""
        try:
            wav_bytes = base64.b64decode(audio_b64)
            with wave.open(io.BytesIO(wav_bytes), "rb") as wf:
                sr = wf.getframerate()
                ch = wf.getnchannels()
                raw = wf.readframes(wf.getnframes())
        except Exception as e:
            self.logger.error(f"Failed to decode WAV: {e}")
            return []

        pcm = np.frombuffer(raw, dtype=np.int16)
        if ch > 1:
            pcm = pcm[::ch]

        # Resample if needed
        if sr != self.sample_rate:
            target_len = int(len(pcm) * self.sample_rate / sr)
            pcm = np.interp(
                np.linspace(0, len(pcm) - 1, target_len),
                np.arange(len(pcm)),
                pcm.astype(np.float64),
            ).astype(np.int16)

        # Split into frames
        frames = []
        for i in range(0, len(pcm), self.frame_samples):
            chunk = pcm[i:i + self.frame_samples]
            if len(chunk) < self.frame_samples:
                chunk = np.pad(chunk, (0, self.frame_samples - len(chunk)))
            frames.append(chunk)
        return frames
