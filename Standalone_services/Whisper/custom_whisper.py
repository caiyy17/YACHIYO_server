from flask import Flask, request, jsonify
import json
import whisper
import time
import os
app = Flask(__name__)

model = whisper.load_model("base")

@app.route('/whisper', methods=['POST'])
def whisper_custom():
    start = time.time()
    if 'file' in request.files:
        file = request.files['file']
        # 你可以在这里保存文件，或者处理文件内容
        time_stamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
        filename = 'tmp/received_file_' + time_stamp + '.wav'
        file.save(filename)
        result = model.transcribe(filename)
        print(result["text"])
        print("Time:", time.time() - start)
        return jsonify({'text': result["text"]})
    else:
        return jsonify({'error': 'No file part in the request'})

if __name__ == '__main__':
    # 建立tmp文件夹
    if not os.path.exists('tmp'):
        os.makedirs('tmp')
    app.run(debug=True, port=5052)