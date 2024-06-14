from flask import Flask, request, jsonify
from transformers import pipeline
app = Flask(__name__)

import deepl
import time

auth_key = "543c9e81-5bb4-4bc8-bf07-5d39428a99ca:fx"  # Replace with your key
translator = deepl.Translator(auth_key)

classifier = pipeline("text-classification", model="j-hartmann/emotion-english-roberta-large")
result = classifier("Nice to meet you")
emotion = result[0]['label']
print(result)
print(emotion)

@app.route("/t2e",methods=['POST'])
def t2e():
    start = time.time()
    data = request.json
    text = data['text']
    language = data['language']
    print("Original text:", text)
    if language == 'en':
        result = classifier(text)
    else:
        result = translator.translate_text(text, target_lang="EN-US")
        print("time translation:", time.time()-start)
        print("Translated text:", result.text)
        result = classifier(result.text)
    print("time total:", time.time()-start)
    print(result)
    if result[0]['score'] < 0.8:
        emotion = 'neutral'
    else:
        emotion = result[0]['label']
    return jsonify({'emotion': emotion})

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5010)