from flask import Flask, request, jsonify
import requests
app = Flask(__name__)

import deepl
import time

auth_key = "543c9e81-5bb4-4bc8-bf07-5d39428a99ca:fx"  # Replace with your key
translator = deepl.Translator(auth_key)

@app.route("/t2e",methods=['POST'])
def t2e():
    start = time.time()
    data = request.json
    text = data['text']
    language = data['language']
    print("Original text:", text)
    if language == 'zh':
        result = classifier(text)
    else:
        result = translator.translate_text(text, target_lang="ZH")
        print("time translation:", time.time()-start)
        print("Translated text:", result.text)
        result = classifier(text)
    if result is None:
        emotion = 'neutral'
    else:
        emotion = result.get('action_number')
    print("time total:", time.time()-start)
    print(result)
    return jsonify({'emotion': emotion})

def classifier(text):
    url = 'http://100.65.144.45:5000/get_action'
    payload = {'query': text}
    response = requests.post(url, json=payload)
    if response.status_code == 200:
        result = response.json()
        return result
    else:
        print("Error:", response.text)
        return None

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5010)