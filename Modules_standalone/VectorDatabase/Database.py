import torch
import faiss
from sentence_transformers import SentenceTransformer


class BGERetriever:
    def __init__(self):
        print("Initializing BGERetriever...")
        self.model = "BAAI/bge-m3"
        device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Using device: {device}")
        self.embedding_model = SentenceTransformer(self.model, device=device)
        test_sentences = ["This is a test sentence.", "Another test sentence for BGERetriever."]
        print("Testing BGERetriever with sample sentences...")
        results = self.encode(test_sentences)
        print(f"Sample results: {results[:2]}")  # Print first two results

    def encode(self, contents):
        with torch.no_grad():
            results = self.embedding_model.encode(contents)
        return results


class SimpleDatabase:
    def __init__(self, name, keys):
        print(f"Initializing SimpleDatabase {name}...")
        self.name = name
        self.keys = keys
        self.build_index()

    def build_index(self):
        print("Loading dataset...")
        print(f"Load {len(self.keys)} keys")
        print("Index generated successfully.")

    def query(self, queries):
        print(f"Queries: {queries}")
        results = []
        for query in queries:
            relevant_items = []
            for i in range(len(self.keys)):
                if self.keys[i] in query:
                    score = 1.0
                    print(f"Key: {self.keys[i]}, Index: {i}, Score: {score}")
                    relevant_items.append({"index": i, "score": float(score)})
            results.append(relevant_items)
        return results


class VectorDatabase:
    def __init__(self, name, retriever, keys):
        print(f"Initializing VectorDatabase {name}...")
        self.name = name
        self.retriever = retriever
        self.keys = keys
        self.build_index()

    def build_index(self):
        print("Loading dataset...")
        self.num_keys = len(self.keys)
        print(f"Load {self.num_keys} keys")

        print("Generating index...")
        embeddings = self.retriever.encode(self.keys)
        index = faiss.IndexFlatIP(embeddings.shape[1])
        index.add(embeddings)
        self.index = index

        print("Index generated successfully.")

    def query(self, queries, k=0, score_threshold=0.5):
        print(f"Queries: {queries}")
        if k == 0:
            k = self.num_keys
        n_query = len(queries)
        query_embedding = self.retriever.encode(queries)
        scores, indices = self.index.search(query_embedding.reshape(n_query, -1), k)
        results = []
        for i in range(n_query):
            relevant_items = []
            for score, idx in zip(scores[i], indices[i]):
                if score < score_threshold:
                    continue
                key = self.keys[idx]
                print(f"Key: {key}, Index: {idx}, Score: {score}")
                relevant_items.append({"index": int(idx), "score": float(score)})
            results.append(relevant_items)
        torch.cuda.empty_cache()
        return results
