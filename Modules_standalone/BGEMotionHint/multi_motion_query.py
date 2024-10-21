import faiss
from sentence_transformers import SentenceTransformer

EMBEDDING_PATH = "./bge-large-zh-v1.5"
embedding_model = SentenceTransformer(EMBEDDING_PATH)

class ActionSemanticRetriever:
    def __init__(self, actions_semantics):
        self.actions_semantics = actions_semantics
        self.index, self.idx_to_action, self.idx_to_semantic = self._build_index()

    def _build_index(self):
        """
        构建语义检索索引。将每个动作的语义文本转化为嵌入，并构建索引以用于最近邻搜索。
        """
        semantics = []
        idx_to_action = {}
        idx_to_semantic = {}
        idx = 0
        for action, semantic_dict in self.actions_semantics.items():
            semantic_list = semantic_dict["actions"]
            for semantic in semantic_list:
                semantics.append(semantic)
                idx_to_action[idx] = action
                idx_to_semantic[idx] = semantic
                idx += 1

        embeddings = embedding_model.encode(semantics)

        index = faiss.IndexFlatIP(embeddings.shape[1])
        index.add(embeddings)

        return index, idx_to_action, idx_to_semantic

    def query_actions(self, query, k=3):
        query_embedding = embedding_model.encode([query])[0]

        scores, indices = self.index.search(query_embedding.reshape(1, -1), k)

        relevant_actions = []
        for score, idx in zip(scores[0], indices[0]):
            action = self.idx_to_action[idx]
            semantic = self.idx_to_semantic[idx]
            relevant_actions.append((action, semantic, score))

        return relevant_actions