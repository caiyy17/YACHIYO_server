import time
import queue
import json

TIMEOUT = 1
MESSAGE_MAX_LENGTH = 200


class CustomLogger:
    def __init__(self, logger, name):
        self.logger = logger
        self.name = name

    def info(self, message, cut=True):
        if cut and len(message) > MESSAGE_MAX_LENGTH:
            message = message[:MESSAGE_MAX_LENGTH] + "..."
        self.logger.info(f"{self.name}: {message}")

    def error(self, message, cut=True):
        if cut and len(message) > MESSAGE_MAX_LENGTH:
            message = message[:MESSAGE_MAX_LENGTH] + "..."
        self.logger.error(f"{self.name}: {message}")


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
    #   - an input_vars entry whose input_name covers each required input
    # Validation is strictly per-node — no cross-module flow modeling; broken
    # links between nodes surface at runtime via the four-state signal rules.
    REQUIRED_CATCH_SIGNALS = []
    REQUIRED_INPUTS = []

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
    def validate_config(cls, config):
        """Static self-check of one node's config against this class's own
        contract — run by the pipeline validator BEFORE any node is built
        (pure classmethod: config + class attributes only; no instance, no
        services). Returns a list of finding strings. Subclasses with extra
        structure extend via super() (see DispatcherStep)."""
        errors = []
        catch_map = cls._check_decl_list(
            "catch_signals", config.get("catch_signals"), errors)
        cls._check_decl_list("pass_signals", config.get("pass_signals"), errors)
        emit_map = cls._check_decl_list(
            "emit_signals", config.get("emit_signals"), errors)

        # catch contract: targets == required, exactly (dispatcher overrides)
        cls._validate_catch_contract(config, set(catch_map.values()), errors)

        # input contract
        input_names = {v.get("input_name")
                       for v in config.get("input_vars", [])}
        for field in cls.required_inputs(config):
            if field not in input_names:
                errors.append(
                    f"module requires input '{field}' (as an input_vars "
                    f"input_name) but it is not declared"
                )

        # emit declaration: exact coverage both ways (the wire-name map is
        # entirely config-defined, no contract default)
        emits = set(cls.emitted_signals(config))
        declared = set(emit_map)
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
        pass_targets = set(
            cls._check_decl_list("pass_signals",
                                 config.get("pass_signals"), []).values())
        for t in sorted(set(emit_map.values()) & pass_targets):
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
    def _check_decl_list(key, entries, errors):
        """Well-formedness of one declaration list: every entry an explicit
        {"source", "target"} object with non-empty values (no shorthand, no
        implicit same-name default), one-to-one — sources unique, targets
        unique. Returns the {source: target} map of the valid entries."""
        decls = {}
        seen_targets = set()
        for e in entries or []:
            if (not isinstance(e, dict)
                    or not e.get("source") or not e.get("target")):
                errors.append(
                    f"{key} entry {e!r} is not an explicit "
                    f'{{"source", "target"}} object with non-empty values'
                )
                continue
            src, tgt = e["source"], e["target"]
            if src in decls:
                errors.append(
                    f"{key} declares source '{src}' more than once — a "
                    f"signal maps to exactly one name"
                )
            if tgt in seen_targets:
                errors.append(
                    f"{key} maps more than one entry to target '{tgt}' — "
                    f"many-to-one renames are an untraceable merge"
                )
            decls[src] = tgt
            seen_targets.add(tgt)
        return decls

    @classmethod
    def required_inputs(cls, config):
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
        kill_event,
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
        self.kill_event = kill_event
        self.config = config or {}  # Mutable settings, defaults to empty dict
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
        the format and uniqueness."""
        return {e["source"]: e["target"] for e in entries or []}

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
        while True:
            if self.kill_event.is_set():
                self.dispose()
                break
            self.check_cancel()
            try:
                data = self.input_queue.get(timeout=TIMEOUT)
                data = json.loads(data)

                # Destination check first: forward pass-through messages immediately
                # (-1 = pipeline exit: matches no node, so it is relayed all the way out)
                dest = data.get("destination", self.index)
                if dest != self.index:
                    self.output_queue.put(json.dumps(data))
                    continue

                # Cancel check: only for messages destined for this node
                if "timestamp" not in data:
                    self.logger.error(f"missing timestamp in data: {data}")
                    continue
                self.current_timestamp = data["timestamp"]
                if self.current_timestamp < self.cancel_timestamp:
                    self.logger.info(f"discarding old data: {data}")
                    self.current_timestamp = None
                    continue

                # Signal handling — every arriving signal MUST be declared:
                #   catch only        -> consume (terminate)
                #   catch + pass      -> consume, then relay (renamed)
                #   pass only         -> relay (renamed), no processing
                #   undeclared        -> wiring error: drop loudly
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
                        self.logger.error(
                            f"undeclared signal '{signal}' at node "
                            f"{self.index}; dropping — declare it in "
                            f"catch_signals or pass_signals"
                        )
                    self.current_timestamp = None
                    continue

                # Normal messages: filter through input_vars/pass_vars
                filtered_data = self.extract_input_data(data)
                pass_data = self.extract_pass_data(data)
                self.logger.info(f"processing data: {filtered_data}")
                self.process(filtered_data, pass_data)
                self.current_timestamp = None
            except queue.Empty:
                self.custom_update()
            except Exception as e:
                self.logger.error(f"{e}")

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
        hasCancel = False
        if not self.cancel_queue.empty():
            while not self.cancel_queue.empty():
                cancel_message = self.cancel_queue.get()
                cancel_message = json.loads(cancel_message)
                self.logger.info(f"received cancel signal: {cancel_message}")
                self.cancel_timestamp = max(
                    self.cancel_timestamp, cancel_message["timestamp"]
                )
                if self.current_timestamp is not None and self.current_timestamp < self.cancel_timestamp:
                    self.logger.info("cancel signal newer than current data, triggered")
                    hasCancel = True
                    self.custom_cancel(cancel_message)
        return hasCancel

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
            input_name = input_var["input_name"]
            source = input_var["source"]
            if source in data:
                extracted_data[input_name] = data[source]

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
            output_name = output_var["output_name"]
            target = output_var["target"]
            if output_name not in self.output_dict:
                self.output_dict[output_name] = []
            self.output_dict[output_name].append(target)

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
        is_log=True,
        direct_send=False,
    ):
        if is_add_destination:
            self.add_destination(data, destination_index)
        if is_add_pass_data:
            self.add_pass_data(data, pass_data)
        elif is_add_timestamp:
            if "timestamp" in pass_data:
                data["timestamp"] = pass_data["timestamp"]

        if direct_send:
            if is_log:
                self.logger.info(f"directly send data: {data}")
            self.send_queue.put(json.dumps(data))
        else:
            if is_log:
                self.logger.info(f"output data: {data}")
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


class FuncA(BaseProcessingStep):
    def custom_init(self):
        self.sleep_time = self.get_config("sleep_time", 0)

    def process(self, data, pass_data={}):
        time.sleep(self.sleep_time)
        output_data = {}
        self.add_output(output_data, "output1", "call_func_a")
        self.output_to_queue(output_data, pass_data)
        return


class FuncB(BaseProcessingStep):
    def custom_init(self):
        self.sleep_time = self.get_config("sleep_time", 0)

    def process(self, data, pass_data={}):
        time.sleep(self.sleep_time)
        output_data = {}
        self.add_output(output_data, "output1", "call_func_b")
        self.output_to_queue(output_data, pass_data)
        return
