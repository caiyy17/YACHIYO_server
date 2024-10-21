from flask import Flask, request, jsonify
from multi_motion_query import ActionSemanticRetriever
import random

import json
import time
datasets_path = "datasets/"
retriever = None
actions_set = None

def initialize_retriever(dataset=""):
    print("initialize_retriever with dataset: ", dataset)
    if dataset == "":
        dataset_path = datasets_path + "default.json"
    else:
        dataset_path = datasets_path + dataset + ".json"

    with open(dataset_path, 'r', encoding='utf-8') as f:
        dataset_json = json.load(f)
        actions_semantics = dataset_json.get('actions_semantics')
        actions_set = dataset_json.get('actions_set')
        retriever = ActionSemanticRetriever(actions_semantics)
    return retriever, actions_set

retriever, actions_set = initialize_retriever()

app = Flask(__name__)

@app.route('/init', methods=['POST'])
def init():
    print("Route /init called")
    start_time = time.time()
    global retriever, actions_set
    data = request.json
    print(data)
    dataset = data.get('dataset')
    if dataset is None:
        return jsonify({"error": "No dataset provided"}), 400
    retriever, actions_set = initialize_retriever(dataset)
    print("Retriever initialized in", time.time() - start_time, "seconds")
    return jsonify({"message": "Retriever initialized with dataset: " + dataset})

@app.route('/get_action', methods=['POST'])
def get_action():
    start_time = time.time()
    print("get_action")
    data = request.json
    query = data.get('query')
    
    if not query:
        return jsonify({"error": "No query provided"}), 400

    # 检索相关动作
    relevant_actions = retriever.query_actions(query)
    if not relevant_actions:
        return jsonify({"error": "No relevant actions found"}), 404
    for action, semantic, score in relevant_actions:
        print(f'Action: {action}, Semantic: {semantic}, Score: {score}')
    
    action_index = relevant_actions[0][0]
    print("get_action in", time.time() - start_time, "seconds")

    # 从actions_set中随机选择一个编号
    if action_index in actions_set:
        action_number = random.choice(actions_set[action_index])
        print("action_number___________", action_number)
        return jsonify({"motion": action_number})
    else:
        return jsonify({"error": "Action index not found in actions_set"}), 404

if __name__ == '__main__':
    app.run(debug=False, port=5054, host="0.0.0.0")