import os
import random
import json
import time
from flask import Flask, request, jsonify
from Database import BGERetriever, SimpleDatabase, VectorDatabase


def initialize_retriever():
    print("Initializing retrievers...")
    retrievers = {}
    retrievers["BGE"] = BGERetriever()
    return retrievers


start_time = time.time()
retrievers = initialize_retriever()
print("Retriever initialized in", time.time() - start_time, "seconds")
datasets_path = "datasets/"
databases = {}
app = Flask(__name__)


@app.route("/load_dataset", methods=["POST"])
def load_dataset():
    global databases, retrievers

    print("Route /load_dataset called")
    start_time = time.time()
    data = request.json
    print(data)
    dataset_name = data.get("dataset")
    dataset_type = data.get("type", "Simple")
    keys = data.get("keys", [])

    if not dataset_name:
        print("No dataset provided")
        return jsonify({"error": "No dataset provided"}), 400
    if dataset_name in databases:
        print("Dataset already loaded, reloading...")

    if dataset_type == "Simple" or keys == []:
        database = SimpleDatabase(dataset_name, keys)
    elif dataset_type == "Vector":
        database = VectorDatabase(dataset_name, list(retrievers.values())[0], keys)
    elif dataset_type == "BGE":
        database = VectorDatabase(dataset_name, retrievers["BGE"], keys)
    else:
        database = SimpleDatabase(dataset_name, keys)

    databases[dataset_name] = database
    print(f"Dataset {dataset_name} loaded in {time.time() - start_time} seconds")
    return jsonify({"message": "Dataset loaded"})


@app.route("/query", methods=["POST"])
def query():
    start_time = time.time()
    print("Route /query called")
    data = request.json
    dataset_name = data.get("dataset")
    if dataset_name not in databases:
        print("Dataset not loaded")
        return jsonify({"error": "Dataset not loaded"}), 400
    queries = data.get("queries")
    if not queries:
        print("No query provided")
        return jsonify({"error": "No query provided"}), 400

    print("dataset_name:", dataset_name)
    database = databases[dataset_name]
    if isinstance(database, SimpleDatabase):
        results = database.query(queries)
    elif isinstance(database, VectorDatabase):
        k = data.get("k", 0)
        score_threshold = data.get("score_threshold", 0.5)
        results = database.query(queries, k=k, score_threshold=score_threshold)
    else:
        results = []

    print("results:", results)
    print("query time:", time.time() - start_time, "seconds")
    return jsonify({"results": results})


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8100)
