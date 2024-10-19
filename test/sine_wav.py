import numpy as np
from pydub import AudioSegment

def generate_sine_wave(frequency, duration_ms, sample_rate=44100, amplitude=0.5):
    # duration_ms：生成音频的时长（以毫秒为单位）
    # sample_rate：采样率，常见值为44100Hz
    # amplitude：振幅，范围为0到1，控制音量
    duration_s = duration_ms / 1000  # 将毫秒转换为秒
    t = np.linspace(0, duration_s, int(sample_rate * duration_s), False)  # 时间数组
    sine_wave = amplitude * np.sin(2 * np.pi * frequency * t)  # 生成正弦波

    # 将正弦波转换为16位PCM格式的数据
    sine_wave_pcm = (sine_wave * (2**15 - 1)).astype(np.int16)

    # 使用pydub将numpy数组转换为音频文件
    audio_segment = AudioSegment(
        sine_wave_pcm.tobytes(), 
        frame_rate=sample_rate, 
        sample_width=sine_wave_pcm.dtype.itemsize, 
        channels=1
    )
    return audio_segment

# 生成一个频率为440Hz，时长为1秒的正弦波
sine_wave_audio = generate_sine_wave(frequency=440, duration_ms=1000)

# 将音频保存为文件
sine_wave_audio.export("test/test.wav", format="wav")