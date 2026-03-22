import json
import os

_CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "configs", "settings")
_settings = None
_secrets = None


def get_settings():
    global _settings
    if _settings is None:
        with open(os.path.join(_CONFIG_DIR, "settings.json")) as f:
            _settings = json.load(f)
    return _settings


def get_secrets():
    global _secrets
    if _secrets is None:
        path = os.path.join(_CONFIG_DIR, "secrets.json")
        if os.path.exists(path):
            with open(path) as f:
                _secrets = json.load(f)
        else:
            _secrets = {}
    return _secrets


def get_setting(*keys):
    """Get a nested setting value by keys."""
    val = get_settings()
    for k in keys:
        val = val[k]
    return val


def get_secret(key, default="EMPTY"):
    """Get a secret value by key."""
    return get_secrets().get(key, default)
