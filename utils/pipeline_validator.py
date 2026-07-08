"""Static pipeline validation, run at init_pipeline time — BEFORE any node
is built, so every finding across all nodes is reported at once (HTTP 400)
at zero build cost (no threads, no service warmups).

This file is pure dispatch: ALL checking logic lives in the step classes
themselves — BaseProcessingStep.validate_config(config) carries the generic
per-node contract (declaration well-formedness, catch targets ==
required_catch_signals(config) exactly, required inputs, emit_signals ==
EMIT_SIGNALS exactly), and modules with extra structure extend it via
super() (DispatcherStep adds its next_nodes/dispatch_vars/dispatch_signals
rules and replaces the catch rule with "referenced by dispatch_signals,
exactly both ways"). validate_config is a pure classmethod: config + class
contract only, no instance, no services.

Validation stays strictly PER-NODE — no cross-module flow modeling; broken
links between nodes surface at runtime (an undeclared signal is dropped
with a loud per-client log entry at the first node it reaches).
"""


def validate_pipeline(pipeline_config, get_class):
    """Returns (errors, warnings): lists of human-readable strings.
    All findings are treated as fatal by the caller; the split is kept for
    reporting granularity."""
    errors, warnings = [], []
    for i, node in enumerate(pipeline_config.get("pipeline", [])):
        func = node["function"]
        name = f"node[{i}] {func}(id={node.get('node_id')})"
        try:
            cls = get_class(func)
        except Exception:
            errors.append(f"{name}: unknown function '{func}'")
            continue
        for finding in cls.validate_config(node.get("config", {})):
            errors.append(f"{name}: {finding}")
    return errors, warnings
