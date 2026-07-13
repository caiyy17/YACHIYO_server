import base64
import json
import struct

from ..base.BaseProcessingStep import BaseProcessingStep


class AudioPadStep(BaseProcessingStep):
    """Pad audio with silence to match motion duration.

    Reads audio_data (base64 WAV) and action (motion JSON with num_frames/framerate).
    If motion duration > audio duration, appends silence to audio.
    Otherwise passes through unchanged.
    """

    @classmethod
    def required_inputs(cls, config):
        # the two inputs are read under config-named keys
        keys = [config.get("audio_key", "audio_data"),
                config.get("action_key", "action")]
        return list(dict.fromkeys(keys))

    def custom_init(self):
        self.audio_key = self.config.get("audio_key", "audio_data")
        self.action_key = self.config.get("action_key", "action")

    def process(self, data, pass_data={}):
        audio_b64 = data.get(self.audio_key, "") or pass_data.get(self.audio_key, "")
        action_raw = data.get(self.action_key, "") or pass_data.get(self.action_key, "")

        # Parse motion duration
        motion_dur = 0
        if action_raw:
            try:
                motion = json.loads(action_raw) if isinstance(action_raw, str) else action_raw
                num_frames = motion.get("num_frames", 0)
                framerate = motion.get("framerate", 30)
                if num_frames > 0 and framerate > 0:
                    motion_dur = num_frames / framerate
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass

        # Parse audio duration and pad if needed
        if audio_b64 and motion_dur > 0:
            try:
                wav_bytes = base64.b64decode(audio_b64)
                audio_dur, padded = self._pad_wav(wav_bytes, motion_dur)
                if padded is not None:
                    self.logger.info(
                        f"Padded audio: {audio_dur:.2f}s -> {motion_dur:.2f}s "
                        f"(+{motion_dur - audio_dur:.2f}s silence)"
                    )
                    pass_data[self.audio_key] = base64.b64encode(padded).decode("ascii")
                else:
                    self.logger.info(
                        f"No pad needed: audio={audio_dur:.2f}s >= motion={motion_dur:.2f}s"
                    )
            except Exception as e:
                self.logger.error(f"Audio pad error: {e}")

        self.output_to_queue({}, pass_data)

    def _pad_wav(self, wav_bytes, target_dur):
        """Returns (audio_duration, padded_bytes_or_None).
        padded_bytes is None if no padding needed."""
        if len(wav_bytes) < 44 or wav_bytes[:4] != b"RIFF":
            return 0, None

        # Parse WAV header
        channels = struct.unpack_from("<H", wav_bytes, 22)[0]
        sample_rate = struct.unpack_from("<I", wav_bytes, 24)[0]
        bits_per_sample = struct.unpack_from("<H", wav_bytes, 34)[0]
        bytes_per_sample = bits_per_sample // 8

        # Find data chunk
        data_offset = wav_bytes.find(b"data")
        if data_offset < 0:
            return 0, None
        data_size = struct.unpack_from("<I", wav_bytes, data_offset + 4)[0]
        pcm_start = data_offset + 8

        audio_dur = data_size / (sample_rate * channels * bytes_per_sample)
        if audio_dur >= target_dur:
            return audio_dur, None

        # Calculate silence to append
        silence_dur = target_dur - audio_dur
        silence_bytes = int(silence_dur * sample_rate * channels * bytes_per_sample)
        silence = b"\x00" * silence_bytes

        new_data_size = data_size + silence_bytes
        new_riff_size = pcm_start + new_data_size - 8

        # Rebuild WAV: header + original PCM + silence
        header = bytearray(wav_bytes[:pcm_start])
        # Update RIFF size
        struct.pack_into("<I", header, 4, new_riff_size)
        # Update data chunk size
        struct.pack_into("<I", header, data_offset + 4, new_data_size)

        return audio_dur, bytes(header) + wav_bytes[pcm_start:pcm_start + data_size] + silence
