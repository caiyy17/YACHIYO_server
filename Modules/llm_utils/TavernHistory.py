import os
import json
import random
import requests

from .SimpleHistory import SimpleHistory
from utils.settings import get_setting
addr_data_query = get_setting("data_query", "data_api")


class Database:
    def __init__(self, name, type, items, logger, k=0, threshold=0.5):
        self.name = name
        self.type = type
        self.items = items
        self.logger = logger
        self.k = k
        self.threshold = threshold
        self.init_dataset()

    def init_dataset(self):
        current_index = 0
        current_key_index = 0
        keys = []
        # global_index → local_index within each item
        self.global_to_local = {}
        for item in self.items:
            item["keys_index"] = []
            for local_idx, key in enumerate(item["keys"]):
                keys.append(key)
                item["keys_index"].append(current_key_index)
                self.global_to_local[current_key_index] = local_idx
                current_key_index += 1
            current_index += 1
        self.keys = keys
        self.num_items = current_index
        self.num_keys = current_key_index

        # init path: a failed load must fail the pipeline init, so the
        # error propagates (custom_init catches it into init_error)
        r = requests.post(
            addr_data_query + "/load_dataset",
            json={
                "dataset": self.name + "_keywords",
                "type": self.type,
                "keys": self.keys,
            },
            timeout=10,
        )
        r.raise_for_status()

    def query(self, prompts):
        if not prompts:
            return []
        try:
            response = requests.post(
                addr_data_query + "/query",
                json={
                    "dataset": self.name + "_keywords",
                    "queries": prompts,
                    "k": self.k,
                    "score_threshold": self.threshold,
                },
                timeout=10,
            )
            results = response.json()["results"]
            score_table = [
                [-1 for _ in range(self.num_keys)] for _ in range(len(prompts))
            ]
            for i in range(len(results)):
                for item in results[i]:
                    index = item["index"]
                    score = item["score"]
                    score_table[i][index] = score

            active_list = []
            for item in self.items:
                scores = [0 for _ in range(len(item["keys_index"]))]
                for global_idx in item["keys_index"]:
                    local_idx = self.global_to_local[global_idx]
                    for i in range(min(item["context_length"], len(prompts))):
                        if score_table[i][global_idx] > item["threshold"]:
                            score_table[i][global_idx] = 1
                            scores[local_idx] = 1
                            break

                if item["logic"] == "and_any":
                    activated = any(scores)
                elif item["logic"] == "and_all":
                    activated = all(scores)
                elif item["logic"] == "not_any":
                    activated = not any(scores)
                elif item["logic"] == "not_all":
                    activated = not all(scores)
                else:
                    activated = False

                if activated:
                    active_list.append(item["index"])
            return active_list
        except Exception as e:
            self.logger.error(f"Database query error: {e}")
            return []


class TavernHistory(SimpleHistory):
    def __init__(self, id, config, logger):
        self.client_id = id
        self.config = config
        self.logger = logger

        self.reset_history = self.config.get("reset_history", True)
        self.history_length = self.config.get("history_length", 20)  # 10 conversation turns
        self.keyword_context_length = self.config.get(
            "keyword_context_length", 2
        )  # 2 conversation turns
        if self.reset_history:
            self.clear_history()
        self.load_history()
        self.extra_info = {}

        self.lorebooks_list = self.config.get("lorebooks", [])
        self.threshold = self.config.get("similarity_threshold", 0.5)
        self.k = self.config.get("k", 0)
        self.lorebooks = self.load_lorebooks()

    def load_lorebooks(self):
        self.logger.info("Loading lorebooks...")
        self.lore_items = []
        self.keywords_lore_items = []
        self.vectorized_lore_items = []
        current_index = 0
        for lorebook in self.lorebooks_list:
            name = lorebook.get("name", "")
            json_file = f"configs/lorebooks/{name}.json"
            self.logger.info(f"Loading lorebook {name} from {json_file}...")
            if not os.path.exists(json_file):
                raise FileNotFoundError(
                    f"lorebook '{name}' not found: {json_file}")
            with open(json_file, "r", encoding="utf-8") as file:
                current_book = json.load(file)
            for item in current_book["data"]:
                if item["strategy"] not in (
                        "constant", "keywords", "vertorized", "both"):
                    raise ValueError(
                        f"lorebook '{name}' item "
                        f"'{item.get('name', '?')}': strategy must be "
                        f"constant/keywords/vertorized/both, got "
                        f"{item['strategy']!r}")
                logic = item.get("logic", "and_any")
                if logic not in ("and_any", "and_all",
                                 "not_any", "not_all"):
                    raise ValueError(
                        f"lorebook '{name}' item "
                        f"'{item.get('name', '?')}': logic must be "
                        f"and_any/and_all/not_any/not_all, got "
                        f"{logic!r}")
                if item["strategy"] == "keywords" or item["strategy"] == "both":
                    lore = {}
                    lore["keys"] = item["keywords"]
                    lore["index"] = current_index
                    lore["context_length"] = item.get(
                        "context_length", self.keyword_context_length
                    )
                    lore["logic"] = item.get("logic", "and_any")
                    lore["threshold"] = item.get("threshold", self.threshold)
                    self.keywords_lore_items.append(lore)
                if item["strategy"] == "vertorized" or item["strategy"] == "both":
                    lore = {}
                    lore["keys"] = item["vectorization_keywords"]
                    lore["index"] = current_index
                    lore["context_length"] = item.get(
                        "context_length", self.keyword_context_length
                    )
                    lore["logic"] = item.get("logic", "and_any")
                    lore["threshold"] = item.get("threshold", self.threshold)
                    self.vectorized_lore_items.append(lore)
                item["order"] += lorebook.get("offset", 0)
                item["content"] = {"role": item["role"], "content": item["content"]}
                self.lore_items.append(item)
                current_index += 1
        self.logger.info(
            f"lore_items: {len(self.lore_items)}, keywords: {len(self.keywords_lore_items)}, vectorized: {len(self.vectorized_lore_items)}"
        )

        # a retrieval database is only built when it has entries: no items
        # means no remote dependency (constant-only lorebooks touch no
        # data_query service at all)
        self.keywords_database = None
        if self.keywords_lore_items:
            self.keywords_database = Database(
                name=f"{self.client_id}_simple",
                type="Simple",
                items=self.keywords_lore_items,
                logger=self.logger,
                k=self.k,
                threshold=self.threshold,
            )
        self.vectorized_database = None
        if self.vectorized_lore_items:
            self.vectorized_database = Database(
                name=f"{self.client_id}_vector",
                type="Vector",
                items=self.vectorized_lore_items,
                logger=self.logger,
                k=self.k,
                threshold=self.threshold,
            )

    def get_activated(self, index, item, results_keywords, results_vectorized):
        rand_float = random.random()
        if item["probability"] < rand_float:
            return False

        if item["strategy"] == "constant":
            return True
        elif item["strategy"] == "keywords":
            if index in results_keywords:
                return True
            else:
                return False
        elif item["strategy"] == "vertorized":
            if index in results_vectorized:
                return True
            else:
                return False
        elif item["strategy"] == "both":
            if index in results_keywords or index in results_vectorized:
                return True
            else:
                return False
        else:
            return False

    def modify_history(self, prompt):
        self.extra_info = {}
        self.extra_info["prompt"] = prompt
        modified_history = self.current_history.copy()
        if prompt is not None:
            modified_history.append({"role": "user", "content": f"{prompt}"})
        queries = []
        for i in range(min(len(modified_history), self.keyword_context_length)):
            if modified_history[-i - 1]["content"] != "":
                queries.append(modified_history[-i - 1]["content"])
        results_keywords = (self.keywords_database.query(queries)
                            if self.keywords_database else [])
        results_vectorized = (self.vectorized_database.query(queries)
                              if self.vectorized_database else [])
        self.logger.info(
            f"keywords: {results_keywords}, vectorized: {results_vectorized}"
        )

        modified_history = [[item] for item in modified_history]
        combined_history = [[] for _ in range(len(modified_history) + 1)]
        current_index = 0
        for item in self.lore_items:
            position = item["position"]
            position = min(
                max(position, -len(modified_history) - 1), len(modified_history)
            )
            if self.get_activated(
                current_index, item, results_keywords, results_vectorized
            ):
                combined_history[position].append(item)
                self.logger.info(
                    f"Item {item['name']} activated, index: {current_index}, position: {position}"
                )
            current_index += 1
        for i in range(len(combined_history)):
            combined_history[i] = sorted(
                combined_history[i], key=lambda x: int(x["order"])
            )
            combined_history[i] = [item["content"] for item in combined_history[i]]
        result = []
        for i in range(len(modified_history)):
            result.append(combined_history[i])
            result.append(modified_history[i])
        result.append(combined_history[-1])
        result = [item for sublist in result for item in sublist]
        # Resolve {{variable}} macros in all messages
        result = self._resolve_macros(result)
        return result
