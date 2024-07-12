from funasr import AutoModel
# from funasr.utils.postprocess_utils import rich_transcription_postprocess

from flask import Flask, request, jsonify
import json
import time
import os
import re
app = Flask(__name__)

model_dir = "iic/SenseVoiceSmall"
model = AutoModel(model=model_dir,
                  vad_model="fsmn-vad",
                  vad_kwargs={"max_single_segment_time": 30000},
                  trust_remote_code=True, device="cuda:0")



@app.route('/whisper', methods=['POST'])
def sense_voice_custom():
    start = time.time()
    if 'file' in request.files:
        file = request.files['file']
        # 你可以在这里保存文件，或者处理文件内容
        time_stamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
        filename = 'tmp/received_file_' + time_stamp + '.wav'
        file.save(filename)
        res = model.generate(
            input=(filename),
            cache={},
            language="zh", # "zn", "en", "yue", "ja", "ko", "nospeech"
            use_itn=False,
            batch_size_s=0, 
        )
        text = res[0]["text"]
        # text = rich_transcription_postprocess(res[0]["text"])
        # 去除<|...|>类似的所有标签
        text = re.sub(r'<\|.*?\|>', '', text)
        print(text)
        print("Time:", time.time() - start)
        return jsonify({'text': text})
    else:
        return jsonify({'error': 'No file part in the request'})

if __name__ == '__main__':
    # 删除tmp文件夹
    if os.path.exists('tmp'):
        os.system('rm -rf tmp')
    os.makedirs('tmp')

    filename = 'test.wav'
    res = model.generate(
        input=(filename),
        cache={},
        language="zh", # "zn", "en", "yue", "ja", "ko", "nospeech"
        use_itn=False,
        batch_size_s=0, 
    )
        
    app.run(debug=True, port=5052)