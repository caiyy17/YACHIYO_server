"""Static pipeline validation, run at init_pipeline time.

Strictly PER-NODE self-consistency — no cross-module flow modeling. Each
check compares one node's config against its own module's declared contract
(class attributes / classmethods on the step class); broken links BETWEEN
nodes are not modeled here and surface at runtime through the four-state
signal rules (undeclared signals are dropped loudly into the client log).

Checks per node:
  1. required_catch_signals(config) must each be covered by a catch_signals
     TARGET (handlers match target names, so renamed wiring still satisfies
     the contract).
  2. required_inputs(config) must each be covered by an input_vars
     input_name.
  3. Node-internal reference consistency:
       - emit_signals sources must be EMIT_SIGNALS of the module
       - dispatcher: >=3 next_nodes; dispatch_vars reference output_vars
         targets; dispatch_signals reference catch_signals targets and match
         the branch count
"""


def _decl_map(entries):
    m = {}
    for e in entries or []:
        if isinstance(e, str):
            m[e] = e
        else:
            m[e["source"]] = e.get("target", e["source"])
    return m


def validate_pipeline(pipeline_config, get_class):
    """Returns (errors, warnings): lists of human-readable strings.
    All findings are treated as fatal by the caller; the split is kept for
    reporting granularity."""
    errors, warnings = [], []
    pipeline = pipeline_config.get("pipeline", [])

    for i, node in enumerate(pipeline):
        func = node["function"]
        c = node.get("config", {})
        name = f"node[{i}] {func}(id={node.get('node_id')})"
        try:
            cls = get_class(func)
        except Exception:
            errors.append(f"{name}: unknown function '{func}'")
            continue

        catch_targets = set(_decl_map(c.get("catch_signals")).values())
        input_names = {v.get("input_name") for v in c.get("input_vars", [])}
        output_targets = {v.get("target") for v in c.get("output_vars", [])}

        # 1. module catch contract
        for sig in cls.required_catch_signals(c) if hasattr(cls, "required_catch_signals") else []:
            if sig not in catch_targets:
                errors.append(
                    f"{name}: module requires catching '{sig}' (as a "
                    f"catch_signals target) but it is not declared"
                )

        # 2. module input contract
        for field in cls.required_inputs(c) if hasattr(cls, "required_inputs") else []:
            if field not in input_names:
                errors.append(
                    f"{name}: module requires input '{field}' (as an "
                    f"input_vars input_name) but it is not declared"
                )

        # 3. node-internal reference consistency
        emits = set(getattr(cls, "EMIT_SIGNALS", []))
        for src in _decl_map(c.get("emit_signals")):
            if src not in emits:
                errors.append(
                    f"{name}: emit_signals renames '{src}' but the module "
                    f"only emits {sorted(emits)}"
                )

        if func == "call_dispatcher":
            nn = c.get("next_nodes", [])
            if len(nn) < 3:
                errors.append(f"{name}: dispatcher needs >=3 next_nodes "
                              f"[branch..., receiver]")
            n_branches = max(len(nn) - 1, 0)
            dv = c.get("dispatch_vars", [])
            if len(dv) != n_branches:
                errors.append(
                    f"{name}: dispatch_vars length {len(dv)} != branch "
                    f"count {n_branches}"
                )
            for bi, names in enumerate(dv):
                for t in names:
                    if t not in output_targets:
                        errors.append(
                            f"{name}: dispatch_vars[{bi}] field '{t}' is not "
                            f"an output_vars target"
                        )
            ds = c.get("dispatch_signals", [])
            if ds and len(ds) > n_branches:
                errors.append(
                    f"{name}: dispatch_signals has {len(ds)} entries for "
                    f"{n_branches} branches"
                )
            for bi, sigs in enumerate(ds):
                for s in sigs:
                    if s not in catch_targets:
                        errors.append(
                            f"{name}: dispatch_signals[{bi}] signal '{s}' is "
                            f"not a catch_signals target"
                        )

    return errors, warnings
