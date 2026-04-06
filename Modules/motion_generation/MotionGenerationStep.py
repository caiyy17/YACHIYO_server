import random
import requests

from ..data_query_base.DataQueryStep import DataQueryStep
from utils.settings import get_setting

addr_motion = get_setting("motion_generation", "addr_motion")


class MotionGenerationCaller:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self.duration = self.config.get("duration", 0)
        self.seed = self.config.get("seed", 42)
        self.use_prompt_engineering = self.config.get("use_prompt_engineering", False)
        self.post_process = self.config.get("post_process", True)
        self.character = self.config.get("character", "")

    def call(self, prompt):
        seed = self.seed if self.seed >= 0 else random.randint(0, 2**32 - 1)
        try:
            body = {
                "text": prompt,
                "duration": self.duration,
                "seed": seed,
                "use_prompt_engineering": self.use_prompt_engineering,
                "post_process": self.post_process,
                "character": self.character,
            }
            if "cfg_scale" in self.config:
                body["cfg_scale"] = self.config["cfg_scale"]
            response = requests.post(
                addr_motion + "/api/generate_json",
                json=body,
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()
            return result
        except Exception as e:
            self.logger.error(f"Failed to call motion generation: {e}")
            return ""


class MotionGenerationStep(DataQueryStep):
    def custom_init(self):
        self.data_query_caller = MotionGenerationCaller(self.config, self.logger)
