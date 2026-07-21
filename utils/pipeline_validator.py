"""Static pipeline validation, run at init_pipeline time — BEFORE any node
is built, so every finding across all nodes is reported at once (HTTP 400)
at zero build cost (no threads, no service warmups).

Per-node checking logic lives in the step classes themselves —
BaseProcessingStep.validate_config(config) carries the generic per-node
contract (declaration well-formedness, catch targets ==
required_catch_signals(config) exactly, required inputs, emit_signals ==
EMIT_SIGNALS exactly, catch_events targets == required_catch_events(config)
exactly), and modules with extra structure extend it via super()
(DispatcherStep adds its next_nodes/dispatch_vars/dispatch_signals rules
and replaces the catch rule with "referenced by dispatch_signals, exactly
both ways"). validate_config is a pure classmethod: config + class
contract only, no instance, no services.

Signal validation stays strictly PER-NODE — no cross-module flow modeling;
broken links between nodes surface at runtime (an undeclared signal is
dropped with a loud per-client log entry at the first node it reaches).
The one pipeline-LEVEL check is the control plane: the top-level `events`
list (the verbs the entry routes to the event handler) must equal the
union of the nodes' non-null catch_events sources, exactly both ways —
"declared means caught somewhere, caught means declared".
"""

# built-in control verbs: always routed and delivered, never declared
BUILTIN_EVENT_VERBS = ("cancel", "kill")


def validate_pipeline(pipeline_config, get_class):
    """Returns (errors, warnings): lists of human-readable strings.
    All findings are treated as fatal by the caller; the split is kept for
    reporting granularity."""
    errors, warnings = [], []
    caught_event_verbs = set()
    for i, node in enumerate(pipeline_config.get("pipeline", [])):
        func = node["function"]
        name = f"node[{i}] {func}(id={node.get('node_id')})"
        nid = node.get("node_id")
        if not isinstance(nid, int) or isinstance(nid, bool) or nid < 1:
            errors.append(f"{name}: node_id must be an int >= 1 "
                          f"(0 is the client's event-source id)")
        for e in node.get("config", {}).get("catch_events") or []:
            if isinstance(e, dict) and isinstance(e.get("source"), str):
                caught_event_verbs.add(e["source"])
        try:
            cls = get_class(func)
        except Exception:
            errors.append(f"{name}: unknown function '{func}'")
            continue
        for finding in cls.validate_config(node.get("config", {})):
            errors.append(f"{name}: {finding}")

    _validate_events(pipeline_config.get("events"), caught_event_verbs,
                     errors)
    return errors, warnings


def _validate_events(events, caught_event_verbs, errors):
    """Top-level `events` list: unique non-empty verb strings, no built-ins,
    equal to the union of the nodes' non-null catch_events sources exactly
    both ways (every declared verb has a catcher; no node catches an
    undeclared verb)."""
    if events is None:
        events = []
    if not isinstance(events, list):
        errors.append(f"top-level 'events' must be a list of verb strings, "
                      f"got {type(events).__name__}")
        return
    declared = set()
    for v in events:
        if not isinstance(v, str) or not v:
            errors.append(f"top-level events entry {v!r} must be a "
                          f"non-empty string")
            continue
        if v in BUILTIN_EVENT_VERBS:
            errors.append(f"top-level events entry '{v}' is a built-in "
                          f"control verb — always routed, never declared")
            continue
        if v in declared:
            errors.append(f"top-level events declares '{v}' more than once")
        declared.add(v)
    for v in sorted(declared - caught_event_verbs):
        errors.append(f"top-level event '{v}' has no catcher — no node "
                      f"declares it as a catch_events source")
    for v in sorted(caught_event_verbs - declared):
        errors.append(f"catch_events source '{v}' is not declared in the "
                      f"top-level events list — the entry would never "
                      f"route it")
