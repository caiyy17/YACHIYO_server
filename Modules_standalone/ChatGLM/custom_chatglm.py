from flask import Flask, request, Response
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from flask import jsonify
import time

import json

app = Flask(__name__)

# 初始化设备和模型
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
# 如果多卡，用最后一张卡
if str(device) == "cuda":
    torch.cuda.set_device(torch.cuda.device_count() - 1)
    device = torch.device("cuda")
print(f"Using device: {device}")
MODEL_PATH = './glm-4-9b-chat'

tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
model = AutoModelForCausalLM.from_pretrained(
    MODEL_PATH,
    low_cpu_mem_usage=True,
    trust_remote_code=True,
    load_in_4bit=True
).eval()

gen_kwargs = {"max_length": 100000, "do_sample": True, "top_k": 1}

@app.route('/chat', methods=['POST'])
def chat():
    data = request.json
    query = data.get("query")
    history = data.get("history")
    print(f"query: {query}")
    if not query:
        return jsonify({"error": "No query provided"}), 400
    
    def generate():
        start = time.time()
        inputs = tokenizer.apply_chat_template( history,
                                                add_generation_prompt=True,
                                                tokenize=True,
                                                return_tensors="pt",
                                                return_dict=True
                                                )
        with torch.no_grad():
            index = len(inputs["input_ids"][0])
            for outputs in model.stream_generate(**inputs, **gen_kwargs):
                outputs = outputs.tolist()[0][index:-1]
                text = tokenizer.decode(outputs, skip_special_tokens=True)
                if text == "":
                    continue
                elif text[-1] == "\n" and index == len(inputs["input_ids"][0]):
                    index += len(outputs)
                    continue
                elif text[-1] == chr(65533):
                    continue
                else:
                    index += len(outputs)
                    print(text, end="")
                    yield json.dumps({"response": text}) + "\n"
        torch.cuda.empty_cache()
        print()
        print(f"Time: {time.time() - start}")
    
    return Response(generate(), content_type='application/json')

if __name__ == '__main__':
    start = time.time()
    hello_query = "Hello to me with only emoji"
    hello_response = ""
    inputs = tokenizer.apply_chat_template([{"role": "user", "content": hello_query}],
                                            add_generation_prompt=True,
                                            tokenize=True,
                                            return_tensors="pt",
                                            return_dict=True
                                            )
    inputs = inputs.to(device)
    with torch.no_grad():
        index = len(inputs["input_ids"][0])
        for outputs in model.stream_generate(**inputs, **gen_kwargs):
            outputs = outputs.tolist()[0][index:-1]
            # print(outputs)
            # if tokenizer.decode(outputs, skip_special_tokens=True) 
            # index += len(outputs)
            text = tokenizer.decode(outputs, skip_special_tokens=True)
            if text == "":
                continue
            elif text[-1] == chr(65533):
                continue
            else:
                index += len(outputs)
                print(tokenizer.decode(outputs, skip_special_tokens=True), end="")
    print()
    print(f"Time: {time.time() - start}")
    app.run(debug=False, host='0.0.0.0', port=5051)
