import os
import importlib
import sys
import traceback


def load_function_map(base_path, base_module):
    function_map = {}
    caller_map = {}
    import_errors = []

    # Iterate through subdirectories in base_path
    for root, dirs, files in os.walk(base_path):
        for dir_name in dirs:
            # Build module path
            module_path = f"{base_module}.{dir_name}"
            try:
                # Dynamically import module
                module = importlib.import_module(module_path)
                # Steps register via `function_map`; callers (streamable
                # generators, composable by name — e.g. by the joint stream
                # node) register via `caller_map`.
                if hasattr(module, "function_map"):
                    function_map.update(getattr(module, "function_map"))
                if hasattr(module, "caller_map"):
                    caller_map.update(getattr(module, "caller_map"))
            except Exception:
                import_errors.append(module_path)
                print(
                    f"Error importing {module_path}:\n{traceback.format_exc()}",
                    file=sys.stderr,
                    end="",
                )

    if import_errors:
        raise RuntimeError(
            f"Failed to import {len(import_errors)} module(s): "
            f"{', '.join(import_errors)}"
        )
    return function_map, caller_map


base_path = os.path.dirname(__file__)  # Current module path
base_module = "Modules"  # Top-level module name
FUNCTION_MAP, CALLER_MAP = load_function_map(base_path, base_module)

print(FUNCTION_MAP)


def get_function_class_by_name(func_name):
    if func_name not in FUNCTION_MAP:
        raise ValueError(f"Unknown function: {func_name}")
    return FUNCTION_MAP[func_name]


def get_caller_class_by_name(caller_name):
    return CALLER_MAP.get(caller_name)
