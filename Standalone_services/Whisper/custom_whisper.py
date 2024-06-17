from flask import Flask, request, jsonify
import json
import whisper
import time
app = Flask(__name__)

model = whisper.load_model("base")

@app.route('/whisper', methods=['POST'])
def whisper_custom():
    start = time.time()
    if 'file' in request.files:
        file = request.files['file']
        # 你可以在这里保存文件，或者处理文件内容
        filename = 'received_file.wav'
        file.save(filename)
        result = model.transcribe(filename)
        print(result["text"])
        print("Time:", time.time() - start)
        return jsonify({'text': result["text"]})
    else:
        return jsonify({'error': 'No file part in the request'})

if __name__ == '__main__':
    app.run(debug=True, port=5001)