import os
import json
import random
import requests

from ..data_query_base.DataQueryStep import DataQueryStep
from utils.settings import get_setting
addr_data_query = get_setting("data_query", "data_api")
datasets_path = get_setting("data_query", "datasets_path")


class LinkDataCaller:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.dataset_name = self.config.get("dataset", "")
        self.type = self.config.get("type", "Simple")
        self.k = self.config.get("k", 0)
        self.score_threshold = self.config.get("score_threshold", 0.5)
        self.return_type = self.config.get("return_type", "any")
        if self.return_type not in ("first", "any", "all"):
            raise ValueError(
                f"return_type must be first/any/all, got {self.return_type!r}")
        self.init_dataset()

    def init_dataset(self):
        self.logger.info("Loading dataset...")
        dataset_path = datasets_path + self.dataset_name + ".json"
        if not os.path.exists(dataset_path):
            raise FileNotFoundError(
                f"dataset '{self.dataset_name}' not found: {dataset_path}")
        with open(dataset_path, "r", encoding="utf-8") as file:
            dataset = json.load(file)
        self.dataset = dataset["data"]

        current_index = 0
        key2index = []
        index2value = {}
        keys = []
        for item in self.dataset:
            for key in item["keys"]:
                keys.append(key)
                key2index.append(current_index)
            index2value[current_index] = item["values"]
            current_index += 1
        self.keys = keys
        self.num_items = current_index
        self.key2index = key2index
        self.index2value = index2value
        self.logger.info(f"Load {self.num_items} items, {len(self.keys)} keys")

        # init path: a failed load must fail the pipeline init, so the
        # error propagates (custom_init catches it into init_error)
        r = requests.post(
            addr_data_query + "/load_dataset",
            json={
                "dataset": self.dataset_name,
                "type": self.type,
                "keys": self.keys,
            },
            timeout=10,
        )
        r.raise_for_status()
        self.logger.info("Dataset loaded successfully")

    def call(self, prompt):
        try:
            response = requests.post(
                addr_data_query + "/query",
                json={
                    "dataset": self.dataset_name,
                    "queries": [prompt],
                    "k": self.k,
                    "score_threshold": self.score_threshold,
                },
                timeout=10,
            )
            result = response.json()["results"][0]
            if len(result) == 0:
                result = ""
            else:
                key_index = result[0]["index"]
                index = self.key2index[key_index]
                values = self.index2value[index]
                if self.return_type == "first":
                    result = values[0]
                elif self.return_type == "any":
                    random_index = random.randint(0, len(values) - 1)
                    result = values[random_index]
                elif self.return_type == "all":
                    result = values
                else:
                    raise ValueError(
                        f"unknown return_type {self.return_type!r}")

            return result
        except Exception as e:
            self.logger.error(f"Failed to call database: {e}")
            return "error"


class LinkDataStep(DataQueryStep):
    def custom_init(self):
        self.data_query_caller = LinkDataCaller(self.config, self.logger)
