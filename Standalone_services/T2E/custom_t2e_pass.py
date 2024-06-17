from flask import Flask, request, jsonify
app = Flask(__name__)
import time

@app.route("/t2e",methods=['POST'])
def t2e():
    start = time.time()
    data = request.json
    text = data['text']
    print("Original text:", text)
    emotion = 'neutral'
    print("Time:", time.time() - start)
    return jsonify({'emotion': emotion})

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5010)