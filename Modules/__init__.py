import os
import importlib


def load_function_map(base_path, base_module):
    function_map = {}

    # Iterate through subdirectories in base_path
    for root, dirs, files in os.walk(base_path):
        for dir_name in dirs:
            # Build module path
            module_path = f"{base_module}.{dir_name}"
            try:
                # Dynamically import module
                module = importlib.import_module(module_path)
                # Check if the module has a `function_map`
                if hasattr(module, "function_map"):
                    function_map.update(getattr(module, "function_map"))
            except ModuleNotFoundError as e:
                print(f"Error importing {module_path}: {e}")
    return function_map


base_path = os.path.dirname(__file__)  # Current module path
base_module = "Modules"  # Top-level module name
FUNCTION_MAP = load_function_map(base_path, base_module)

print(FUNCTION_MAP)


def get_function_class_by_name(func_name):
    return FUNCTION_MAP.get(func_name, FUNCTION_MAP["default"])
