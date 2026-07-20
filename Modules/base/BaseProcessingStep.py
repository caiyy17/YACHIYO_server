import time
import queue
import json

TIMEOUT = 1
MESSAGE_MAX_LENGTH = 200


class CustomLogger:
    """Name-prefixed, truncating logger with per-message verbosity levels.
    A message prints when its level >= the pipeline's __debug_level
    threshold. Defaults: info 1, warning 5, error 10; per-chunk stream
    logs pass level=0, so the default threshold (1) hides them and
    __debug_level: 0 shows everything."""

    def __init__(self, logger, name, threshold=1):
        self.logger = logger
        self.name = name
        self.threshold = threshold

    def _format(self, message, cut):
        if cut and len(message) > MESSAGE_MAX_LENGTH:
            message = message[:MESSAGE_MAX_LENGTH] + "..."
        return f"{self.name}: {message}"

    def info(self, message, cut=True, level=1):
        if level >= self.threshold:
            self.logger.info(self._format(message, cut))

    def warning(self, message, cut=True, level=5):
        if level >= self.threshold:
            self.logger.warning(self._format(message, cut))

    def error(self, message, cut=True, level=10):
        if level >= self.threshold:
            self.logger.error(self._format(message, cut))


class BaseProcessingStep:
    # Signals this module emits, by INTERNAL name (module contract, mirrors
    # how handlers own their internal catch names). The wire name of each
    # comes from the config emit_signals declaration — every entry here MUST
    # be declared there (validator enforces coverage at init; no default).
    # Renaming emitted envelope signals is what makes nested
    # dispatcher/receiver brackets wireable (inner pair uses distinct names).
    EMIT_SIGNALS = []
    # Module SELF-consistency contracts, checked statically at pipeline init
    # (utils/pipeline_validator): the node's config must declare
    #   - a catch_signals entry whose TARGET covers each required catch name
    #     (handlers match target names, so renamed wiring still satisfies it)
    #   - an input_vars entry whose TARGET covers each required input
    # Validation is strictly per-node — no cross-module flow modeling; broken
    # links between nodes surface at runtime via the four-state signal rules.
    REQUIRED_CATCH_SIGNALS = []
    # The full input contract: every internal name the module reads.
    # Configs must declare each one; wiring "source": null explicitly
    # falls back to the module default for that input.
    REQUIRED_INPUTS = []
    # Every product name the module can add_output — the output contract:
    # configs must declare each one, wiring "target": null to explicitly
    # drop it from the outgoing message.
    OUTPUTS = []
    # Free-form interfaces (config-defined names the class cannot know):
    # extra input targets (e.g. the splitter's data-lane keys) / extra
    # output sources (e.g. the collector's demux keys) are then allowed
    # beyond the declared contract.
    FREE_INPUTS = False
    FREE_OUTPUTS = False
    # Verbosity level of per-message input logs ("processing data"):
    # 0 = hidden at the default threshold (outputs, signals and
    # milestones still log at 1); raise per module if its input is
    # worth recording.
    CONTENT_LOG_LEVEL = 0

    @classmethod
    def required_catch_signals(cls, config):
        """Override for config-dependent contracts (e.g. continuous mode).
        validate_config enforces catch targets == this, exactly (a target
        the module does not handle would be silently swallowed)."""
        return list(cls.REQUIRED_CATCH_SIGNALS)

    @classmethod
    def emitted_signals(cls, config):
        """Override for config-dependent emit contracts (e.g. stream mode).
        validate_config enforces emit_signals sources == this, exactly."""
        return list(cls.EMIT_SIGNALS)

    @classmethod
    def module_outputs(cls, config):
        """Override for config-dependent output contracts (e.g. the joint
        stream's streams table). validate_config enforces output_vars
        sources == this, exactly (plus free-form when FREE_OUTPUTS)."""
        return list(cls.OUTPUTS)

    @classmethod
    def validate_config(cls, config):
        """Static self-check of one node's config against this class's own
        contract — run by the pipeline validator BEFORE any node is built
        (pure classmethod: config + class attributes only; no instance, no
        services). Returns a list of finding strings. Subclasses with extra
        structure extend via super() (see DispatcherStep)."""
        errors = []
        # signals: the wire side may be an explicit null opt-out — catch
        # source null = declared but not wired (never received), emit
        # target null = declared but suppressed (not sent). pass has no
        # contract to opt out of: both sides must be real names.
        catch_pairs = cls._check_decl_list(
            "catch_signals", config.get("catch_signals"), errors,
            null_side="source")
        cls._check_decl_list("pass_signals", config.get("pass_signals"), errors)
        emit_pairs = cls._check_decl_list(
            "emit_signals", config.get("emit_signals"), errors,
            null_side="target")

        # catch contract: targets == required, exactly (dispatcher overrides)
        cls._validate_catch_contract(
            config, {t for _, t in catch_pairs}, errors)

        # input contract: input_vars targets == required_inputs(config),
        # exactly (FREE_INPUTS allows extra free-form targets, e.g. data
        # lanes). The wire side (source) may be an explicit null = "use
        # the module default for this input".
        required = set(cls.required_inputs(config))
        in_map = cls._check_var_list(
            "input_vars", config.get("input_vars"), "source", errors)
        for field in sorted(required - set(in_map)):
            errors.append(
                f"module input '{field}' is not declared in input_vars "
                f'(wire it, or declare it with "source": null to use the '
                f"module default)"
            )
        if not cls.FREE_INPUTS:
            for t in sorted(set(in_map) - required):
                errors.append(
                    f"input_vars target '{t}' is not an input this module "
                    f"reads ({sorted(required) or 'none'})"
                )

        # output contract: output_vars sources == module_outputs, exactly
        # (FREE_OUTPUTS allows extra free-form sources, e.g. demux keys).
        # The wire side (target) may be an explicit null = "drop this
        # product from the outgoing message".
        outputs = set(cls.module_outputs(config))
        out_map = cls._check_var_list(
            "output_vars", config.get("output_vars"), "target", errors)
        for field in sorted(outputs - set(out_map)):
            errors.append(
                f"module output '{field}' is not declared in output_vars "
                f'(wire it, or declare it with "target": null to drop it)'
            )
        if not cls.FREE_OUTPUTS:
            for s in sorted(set(out_map) - outputs):
                errors.append(
                    f"output_vars source '{s}' is not an output this module "
                    f"produces ({sorted(outputs) or 'none'})"
                )

        # emit declaration: exact coverage both ways (the wire-name map is
        # entirely config-defined, no contract default)
        emits = set(cls.emitted_signals(config))
        declared = {s for s, _ in emit_pairs}
        for src in sorted(declared - emits):
            errors.append(
                f"emit_signals declares '{src}' but the module only emits "
                f"{sorted(emits)}"
            )
        for s in sorted(emits - declared):
            errors.append(
                f"module emits '{s}' but it is not declared in emit_signals"
            )

        # outgoing wire-name clash: an emitted wire name colliding with a
        # relayed (pass) wire name would give downstream two
        # indistinguishable signals — an untraceable merge, same ban as
        # many-to-one renames (rename one side via config)
        pass_targets = {t for _, t in cls._check_decl_list(
            "pass_signals", config.get("pass_signals"), [])}
        emit_targets = {t for _, t in emit_pairs if t is not None}
        for t in sorted(emit_targets & pass_targets):
            errors.append(
                f"emit_signals target '{t}' collides with a pass_signals "
                f"target — downstream could not tell the emitted signal "
                f"from the relayed one; rename one of them"
            )
        return errors

    @classmethod
    def _validate_catch_contract(cls, config, catch_targets, errors):
        """catch targets must equal required_catch_signals(config) exactly:
        fewer -> the module cannot work; more -> process() would silently
        swallow the extra name."""
        required = set(cls.required_catch_signals(config))
        for sig in sorted(required - catch_targets):
            errors.append(
                f"module requires catching '{sig}' (as a catch_signals "
                f"target) but it is not declared"
            )
        for t in sorted(catch_targets - required):
            errors.append(
                f"catch_signals target '{t}' is not a signal this module "
                f"handles ({sorted(required) or 'none'}) — process() would "
                f"silently swallow it"
            )

    @staticmethod
    def _check_decl_list(key, entries, errors, null_side=None):
        """Well-formedness of one signal declaration list: every entry an
        explicit {"source", "target"} object (no shorthand, no implicit
        same-name default), one-to-one — sources unique, targets unique.
        `null_side` names the wire side that may be an explicit null
        opt-out ("source" for catch = declared but never wired, "target"
        for emit = declared but suppressed); the other side is always a
        non-empty string. Nulls are exempt from uniqueness. Returns the
        (source, target) pairs of the valid entries."""
        pairs = []
        seen_sources, seen_targets = set(), set()
        for e in entries or []:
            if not isinstance(e, dict) or "source" not in e or "target" not in e:
                errors.append(
                    f"{key} entry {e!r} is not an explicit "
                    f'{{"source", "target"}} object'
                )
                continue
            src, tgt = e["source"], e["target"]
            src_ok = (isinstance(src, str) and src) \
                or (src is None and null_side == "source")
            tgt_ok = (isinstance(tgt, str) and tgt) \
                or (tgt is None and null_side == "target")
            if not src_ok or not tgt_ok:
                errors.append(
                    f"{key} entry {e!r} needs non-empty string values"
                    + (f" (only '{null_side}' may be an explicit null)"
                       if null_side else "")
                )
                continue
            if src is not None:
                if src in seen_sources:
                    errors.append(
                        f"{key} declares source '{src}' more than once — a "
                        f"signal maps to exactly one name"
                    )
                seen_sources.add(src)
            if tgt is not None:
                if tgt in seen_targets:
                    errors.append(
                        f"{key} maps more than one entry to target '{tgt}' "
                        f"— many-to-one renames are an untraceable merge"
                    )
                seen_targets.add(tgt)
            pairs.append((src, tgt))
        return pairs

    @staticmethod
    def _check_var_list(key, entries, null_side, errors):
        """Well-formedness of input_vars/output_vars: every entry an
        explicit {"source", "target"} object; the contract side (target
        for inputs, source for outputs) is a non-empty string, the wire
        side (`null_side`) is a non-empty string or an explicit null
        opt-out. One-to-one — contract names unique, non-null wire names
        unique. Returns {contract name: wire name or None}."""
        contract_side = "target" if null_side == "source" else "source"
        decls = {}
        seen_wire = set()
        for e in entries or []:
            if not isinstance(e, dict) or "source" not in e or "target" not in e:
                errors.append(
                    f"{key} entry {e!r} is not an explicit "
                    f'{{"source", "target"}} object'
                )
                continue
            contract, wire = e[contract_side], e[null_side]
            if not contract or not isinstance(contract, str):
                errors.append(
                    f"{key} entry {e!r}: '{contract_side}' must be a "
                    f"non-empty string (the null opt-out goes on the "
                    f"'{null_side}' side)"
                )
                continue
            if wire is not None and (not wire or not isinstance(wire, str)):
                errors.append(
                    f"{key} entry {e!r}: '{null_side}' must be a non-empty "
                    f"string or an explicit null"
                )
                continue
            if contract in decls:
                errors.append(
                    f"{key} declares {contract_side} '{contract}' more "
                    f"than once"
                )
            if wire is not None:
                if wire in seen_wire:
                    errors.append(
                        f"{key} maps {null_side} '{wire}' more than once — "
                        f"one-to-one only"
                    )
                seen_wire.add(wire)
            decls[contract] = wire
        return decls

    @classmethod
    def required_inputs(cls, config):
        """The full input contract (override for config-dependent inputs,
        e.g. the joint stream's streams table). Every name must be declared
        in input_vars; any may be wired to null for the module default."""
        return list(cls.REQUIRED_INPUTS)

    def __init__(
        self,
        index,
        client_id,
        logger,
        send_queue,
        input_queue,
        output_queue,
        cancel_queue,
        config=None,
    ):
        self.name = self.__class__.__name__
        self.index = index
        self.client_id = client_id
        self.logger = CustomLogger(logger, self.name)
        self.send_queue = send_queue
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.cancel_queue = cancel_queue
        self.cancel_timestamp = 0
        self.current_timestamp = None
        # Set when a {"signal": "kill"} verb arrives on the cancel queue —
        # sticky (a queue message is consumed once, unlike an event flag);
        # the run loop exits on it and calls dispose()/custom_dispose().
        self._killed = False
        self.config = config or {}  # Mutable settings, defaults to empty dict
        # Pipeline-wide log threshold (config top-level __debug_level,
        # injected per node at init): messages print when their level >=
        # this. 0 shows per-chunk stream logs, 1 (default) hides them,
        # 5/10 keep only warnings/errors.
        self.debug_level = int(self.config.get("__debug_level", 1) or 0)
        self.logger.threshold = self.debug_level
        self.reserved_input_vars = [
            "destination",
            "signal",
            "timestamp",
        ]
        self.output_dict = {}
        # Signal routing — declared ENTIRELY in config, like input_vars:
        #   catch_signals: deliver to process() (renamed to target)
        #   pass_signals:  relay downstream (renamed to target); when combined
        #                  with catch, relay happens AFTER process() returns
        # Entries are explicit {"source": arriving_name, "target": new_name}
        # objects — both fields spelled out, same-name included (no
        # shorthand, no implicit defaults). Declarations are ONE-TO-ONE maps
        # (unique sources and unique targets per list — the validator
        # enforces format and uniqueness): a signal is delivered once,
        # relayed once, emitted once, under exactly one name. Every signal
        # arriving at a node MUST be declared (catch and/or pass);
        # undeclared signals are dropped with an error.
        self.catch_signal_map = self._parse_signal_decls(
            self.config.get("catch_signals"))
        self.pass_signal_map = self._parse_signal_decls(
            self.config.get("pass_signals"))
        self.catch_signal_set = set(self.catch_signal_map)
        self.pass_signal_set = set(self.pass_signal_map)
        # emit map: internal name -> wire name, ENTIRELY from config — same
        # rule as catch/pass, no contract default. The validator enforces at
        # init that every EMIT_SIGNALS entry is declared.
        self.emit_signal_map = self._parse_signal_decls(
            self.config.get("emit_signals"))
        self.prepare_output_dict()
        self._init_with_timeout()
        self.logger.info("initialized")

    @staticmethod
    def _parse_signal_decls(entries):
        """Explicit {"source", "target"} entries -> {source: target}.
        No shorthand and no implicit defaults: every declaration spells out
        both names (same-name included), one-to-one — the validator enforces
        the format and uniqueness. A null source (catch opt-out: declared
        but never wired) is skipped — there is no wire name to match; a
        null target (emit opt-out) is kept so emit_signal can tell an
        explicit suppression from an undeclared name."""
        return {e["source"]: e["target"] for e in entries or []
                if e.get("source") is not None}

    def _init_with_timeout(self, timeout=60):
        """Run custom_init with a timeout to prevent hanging on unreachable
        services. The outcome is recorded in self.init_error (None = OK) so
        the pipeline builder can fail fast and surface it to the client."""
        import threading
        init_done = threading.Event()
        init_error = [None]
        self.init_error = None

        def _run_init():
            try:
                self.custom_init()
            except Exception as e:
                init_error[0] = e
            finally:
                init_done.set()

        t = threading.Thread(target=_run_init, daemon=True)
        t.start()
        if not init_done.wait(timeout=timeout):
            self.init_error = f"custom_init timed out after {timeout}s"
            self.logger.error(self.init_error)
        elif init_error[0]:
            self.init_error = f"custom_init failed: {init_error[0]}"
            self.logger.error(self.init_error)

    def custom_init(self):
        """Subclasses can override this method for custom initialization."""
        pass

    def custom_update(self):
        """Called every loop iteration when no message is available (queue empty).
        Subclasses can override for timer-based logic, periodic checks, etc."""
        pass

    def run(self):
        while not self._killed:
            try:
                # The whole iteration shares one error boundary. queue.Empty
                # is the normal no-input branch; custom_update still runs
                # inside the outer boundary so its failures cannot kill the
                # node thread.
                self.check_cancel()
                if self._killed:
                    break
                try:
                    data = self.input_queue.get(timeout=TIMEOUT)
                except queue.Empty:
                    self.custom_update()
                    continue
                data = json.loads(data)

                # Destination check first: forward pass-through messages immediately
                # (-1 = pipeline exit: matches no node, so it is relayed all the way out)
                dest = data.get("destination", self.index)
                if dest != self.index:
                    self.output_queue.put(json.dumps(data))
                    continue

                # Cancel check: only for messages destined for this node.
                # A timestamp is guaranteed: the entry validates client
                # messages, and internal outputs are stamped via stamp().
                self.current_timestamp = data["timestamp"]
                if self.current_timestamp < self.cancel_timestamp:
                    self.logger.info(f"discarding old data: {data}")
                    self.current_timestamp = None
                    continue

                # Signal handling — every arriving signal MUST be declared:
                #   catch only        -> consume (terminate)
                #   catch + pass      -> consume, then relay (renamed)
                #   pass only         -> relay (renamed), no processing
                #   undeclared        -> warn + drop (a signal addressed to
                #                        a node that doesn't want it dies here)
                # This applies to directed signals too (destination surviving
                # here equals self.index): the relayed copy is re-addressed to
                # this node's first edge, so signals travel edge by edge
                # exactly like data output, checked at every hop.
                signal = data.get("signal", "")
                if signal != "":
                    caught = signal in self.catch_signal_set
                    if caught:
                        filtered_data = {
                            k: v for k, v in data.items()
                            if k not in ("destination",)
                        }
                        filtered_data["signal"] = self.catch_signal_map[signal]
                        self.logger.info(f"processing data: {filtered_data}")
                        self.process(filtered_data,
                                     {"timestamp": data.get("timestamp")})
                    # Consume-then-relay: relay AFTER process() returns, so
                    # anything the node emitted for the signal stays ahead
                    # of the relayed copy.
                    if signal in self.pass_signal_set:
                        self._relay_caught(data, signal)
                    elif not caught:
                        self.logger.warning(
                            f"undeclared signal '{signal}' at node "
                            f"{self.index}; dropped (declare it in "
                            f"catch_signals or pass_signals to handle "
                            f"or forward)"
                        )
                    self.current_timestamp = None
                    continue

                # Normal messages: filter through input_vars/pass_vars
                filtered_data = self.extract_input_data(data)
                pass_data = self.extract_pass_data(data)
                self.logger.info(f"processing data: {filtered_data}",
                                 level=self.CONTENT_LOG_LEVEL)
                self.process(filtered_data, pass_data)
                self.current_timestamp = None
            except Exception as e:
                self.logger.error(
                    f"run iteration failed; dropped current message: "
                    f"{type(e).__name__}: {e}"
                )
                self.current_timestamp = None
        try:
            self.dispose()
        except Exception as e:
            self.logger.error(
                f"dispose failed: {type(e).__name__}: {e}"
            )

    def _relay_caught(self, data, signal):
        """Relay an arriving signal along its pass declaration: one copy,
        renamed to the declared target."""
        relay = {k: v for k, v in data.items() if k != "destination"}
        relay["signal"] = self.pass_signal_map[signal]
        self._relay_signal(relay)

    def _relay_signal(self, relay):
        """Hook: where a passed signal goes. Default: this node's first edge
        (next_nodes[0]) — the same default destination a data output gets,
        so signals travel edge by edge like everything else. DispatcherStep
        overrides this to direct the relay to its receiver."""
        self.add_destination(relay, 0)
        self.output_queue.put(json.dumps(relay))

    def check_cancel(self):
        """Drain the control queue. Two verbs share it:
          cancel — void content older than its stamp (custom_cancel hook)
          kill   — the node must exit: sets the sticky _killed flag; the run
                   loop breaks on it and calls dispose()/custom_dispose().
        Returns True when in-flight work must be abandoned — kill always,
        cancel only when it actually voids the current turn (its stamp is
        newer than current_timestamp) — so in-processing polling points
        (e.g. streaming loops) abort exactly when needed."""
        hasCancel = False
        if not self.cancel_queue.empty():
            while not self.cancel_queue.empty():
                cancel_message = self.cancel_queue.get()
                cancel_message = json.loads(cancel_message)
                if cancel_message.get("signal") == "kill":
                    self.logger.info("received kill signal")
                    self._killed = True
                    hasCancel = True
                    continue
                # cancel always carries a numeric stamp: the entry validates
                # client cancels, and internal ones (teardown) are trusted
                self.logger.info(f"received cancel signal: {cancel_message}")
                self.cancel_timestamp = max(
                    self.cancel_timestamp, cancel_message["timestamp"]
                )
                if self.current_timestamp is not None and self.current_timestamp < self.cancel_timestamp:
                    self.logger.info("cancel signal newer than current data, triggered")
                    hasCancel = True
                    self.custom_cancel(cancel_message)
        # _killed keeps polling points aborting even after the kill verb
        # itself was consumed (a queue message is read once; the flag is
        # the sticky truth)
        return hasCancel or self._killed

    def custom_cancel(self, cancel_message):
        """Subclasses can override this method for custom cancel handling."""
        pass

    def dispose(self):
        self.logger.info("disposing")
        self.custom_dispose()

    def custom_dispose(self):
        """Subclasses can override this method for custom cleanup."""
        pass

    def extract_input_data(self, data):
        """
        Extract required key-value pairs from input data.
        Based on input_vars in config, extract specified variables from data.
        :param data: Output from the previous node (dict)
        :return: Extracted input data dict
        """
        extracted_data = {}

        if "signal" in data and data["signal"] != "":
            extracted_data["signal"] = data["signal"]
        if "timestamp" in data:
            extracted_data["timestamp"] = data["timestamp"]

        input_vars = self.config.get("input_vars", [])
        for input_var in input_vars:
            # {source, target}: read incoming field `source`, expose it to the
            # module under internal name `target`. An explicit null source is
            # the declared opt-out: nothing is wired, the module default applies.
            source = input_var["source"]
            target = input_var["target"]
            if source is not None and source in data:
                extracted_data[target] = data[source]

        return extracted_data

    def extract_pass_data(self, data):
        """
        Extract required key-value pairs from input data.
        Based on pass_vars in config, extract specified variables from data.
        :param data: Output from the previous node (dict)
        :return: Extracted pass-through data dict
        """
        extracted_data = {}

        if "timestamp" in data:
            extracted_data["timestamp"] = data["timestamp"]

        pass_vars = self.config.get("pass_vars", [])
        for pass_var in pass_vars:
            target = pass_var["target"]
            source = pass_var["source"]
            if source in data:
                extracted_data[target] = data[source]

        return extracted_data

    def add_pass_data(self, data, pass_data):
        # Add pass_data entries into data
        for key, value in pass_data.items():
            if key not in data:
                data[key] = value
        return data

    def stamp(self, msg, ctx):
        """Copy the turn IDENTITY (the reserved timestamp) from an
        upstream context (pass_data-shaped dict) onto msg.
        Modules never handle raw clock values: identity always originates
        upstream (the WS entry stamps are the client's, the WebRTC gateway
        mints its own), and this is the single place it is written. A
        context without a timestamp simply leaves msg unstamped."""
        ts = (ctx or {}).get("timestamp")
        if ts is not None:
            msg["timestamp"] = ts
        return msg

    def envelope(self, msg, pass_data, wrap=None):
        """Attach the CONTENT side of a pass-through context onto msg
        (identity is stamp()'s job — timestamp is excluded here). Two wire
        shapes exist: data messages carry pass fields FLAT (what the next
        node's extract reads), signal messages wrap them under "pass_data"
        (keeps the signal schema fixed). wrap defaults by message kind."""
        content = {k: v for k, v in (pass_data or {}).items()
                   if k != "timestamp"}
        if not content:
            return msg
        if wrap is None:
            wrap = bool(msg.get("signal"))
        if wrap:
            msg["pass_data"] = content
        else:
            for key, value in content.items():
                if key not in msg:
                    msg[key] = value
        return msg

    def add_destination(self, data, index=0):
        """Resolve destination by looking up next_nodes[index].

        next_nodes entries are node ids, or -1 for the pipeline exit (the
        message is forwarded untouched by every node and leaves to the client).
        Terminal nodes must declare next_nodes: [-1]."""
        destination = self.config.get("next_nodes", [])
        if len(destination) == 0:
            raise ValueError(
                f"{self.name}: next_nodes is empty; terminal nodes must declare [-1]"
            )
        if index < 0 or index >= len(destination):
            raise ValueError(
                f"{self.name}: destination index {index} is invalid, "
                f"should be 0..{len(destination) - 1}"
            )
        data["destination"] = destination[index]
        return data

    def prepare_output_dict(self):
        output_vars = self.config.get("output_vars", [])
        self.output_dict = {}

        for output_var in output_vars:
            # {source, target}: the module produces value `source`, which is
            # placed into the outgoing message under field name `target`. An
            # explicit null target is the declared opt-out: the product is
            # dropped from the outgoing message.
            source = output_var["source"]
            target = output_var["target"]
            if target is None:
                continue
            if source not in self.output_dict:
                self.output_dict[source] = []
            self.output_dict[source].append(target)

    def add_output(self, output_data, key, value):
        if key not in self.output_dict:
            return
        for target in self.output_dict[key]:
            if target not in output_data:
                output_data[target] = value

    def output_to_queue(
        self,
        data,
        pass_data={},
        *,
        is_add_timestamp=True,
        is_add_destination=True,
        destination_index=0,
        is_add_pass_data=True,
        log_level=1,
        direct_send=False,
    ):
        if is_add_destination:
            self.add_destination(data, destination_index)
        if is_add_pass_data:
            self.add_pass_data(data, pass_data)
        elif is_add_timestamp:
            # identity-only attach: the context stamps, content stays behind
            self.stamp(data, pass_data)

        if direct_send:
            self.logger.info(f"directly send data: {data}",
                             level=log_level)
            self.send_queue.put(json.dumps(data))
        else:
            self.logger.info(f"output data: {data}",
                             level=log_level)
            self.output_queue.put(json.dumps(data))
        return

    def emit_signal(self, internal_name, pass_data={}, **kwargs):
        """Emit a signal by its INTERNAL name; the wire name comes from the
        emit_signals config declaration. By default the signal is addressed
        to this node's first edge, like any data output; pass
        destination_index for a directed signal (dispatcher envelope). Any
        pass-through data rides FLAT on the signal message — the same shape
        as a data message. An undeclared internal name is a wiring error:
        error + drop, same spirit as the receiving-side four-state rule
        (the validator catches this at init)."""
        if internal_name in self.emit_signal_map \
                and self.emit_signal_map[internal_name] is None:
            return  # declared with a null target: explicit suppression
        wire_name = self.emit_signal_map.get(internal_name)
        if wire_name is None:
            self.logger.error(
                f"emit_signal('{internal_name}') is not declared in "
                f"emit_signals at node {self.index}; dropping — declare it "
                f"in the node config"
            )
            return
        self.output_to_queue({"signal": wire_name}, pass_data, **kwargs)

    def process(self, data, pass_data={}):
        """Process the extracted data. Subclasses can override this method."""
        output_data = {}
        self.add_output(output_data, "result", f"Processed by {self.name}")
        self.output_to_queue(output_data, pass_data)
        return

    def get_config(self, key, default=None):
        """Get a specific config value."""
        return self.config.get(key, default)


class DefaultStep(BaseProcessingStep):
    """The 'default' registry entry: base behavior with its own output
    contract (kept off the base class so subclasses do not inherit a
    phantom 'result' output)."""
    OUTPUTS = ["result"]


class FuncA(BaseProcessingStep):
    OUTPUTS = ["output1"]

    def custom_init(self):
        self.sleep_time = self.get_config("sleep_time", 0)

    def process(self, data, pass_data={}):
        time.sleep(self.sleep_time)
        output_data = {}
        self.add_output(output_data, "output1", "call_func_a")
        self.output_to_queue(output_data, pass_data)
        return


class FuncB(BaseProcessingStep):
    OUTPUTS = ["output1"]

    def custom_init(self):
        self.sleep_time = self.get_config("sleep_time", 0)

    def process(self, data, pass_data={}):
        time.sleep(self.sleep_time)
        output_data = {}
        self.add_output(output_data, "output1", "call_func_b")
        self.output_to_queue(output_data, pass_data)
        return
