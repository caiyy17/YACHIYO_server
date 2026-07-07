"""
Dispatcher node: splits one message into parallel branch messages.

Uses standard input_vars/output_vars/pass_vars:
    input_vars:  fields to dispatch (renamed from upstream)
    output_vars: maps input names to target names for branches
    pass_vars:   metadata forwarded to receiver via dispatch_start

Config:
    next_nodes: [branch_0_id, branch_1_id, ..., receiver_id]
        The last entry is always the receiver node.
    dispatch_vars: [
        ["renamed_field"],   # branch 0 fields, by output_vars TARGET name
        ["other_field"],     # branch 1 fields (same rule as dispatch_signals:
    ]                        #  reference the renamed/wire name)
    dispatch_signals: [      # optional; parallel to dispatch_vars
        ["renamed_sig"],     # signals (by their catch TARGET name) directed
        [],                  # to each branch as DIRECTED signal messages
    ]
        Every name here must be a catch_signals target of this node.

Signal semantics (the dispatcher's logical mainline is the RECEIVER):
    pass_signals  -> relayed DIRECTED TO THE RECEIVER (renamed), transiting
                     the branch nodes via destination routing — branches
                     neither see nor declare them. The receiver declares its
                     own pass to continue the signal downstream.
    dispatch_signals -> per-branch directed copies (by catch target name);
                     use catch renaming to keep the branch-directed name
                     distinct so a signal can go to branches and receiver
                     under different names without double-triggering.

Emit order:
    1. signal "dispatch_start" + pass_data -> receiver
    2. Branch messages in REVERSE topological order (later branch first)
    3. signal "dispatch_end" -> receiver
"""

from ..base.BaseProcessingStep import BaseProcessingStep


class DispatcherStep(BaseProcessingStep):
    # Directed group envelope to the receiver. Renaming these via config
    # emit_signals (and catching the renamed pair at the inner receiver) is
    # what makes NESTED dispatcher/receiver brackets wireable without the
    # inner envelope colliding with the outer one.
    EMIT_SIGNALS = ["dispatch_start", "dispatch_end"]

    def custom_init(self):
        nodes = self.config.get("next_nodes", [])
        if len(nodes) < 3:
            self.logger.error(
                "Dispatcher needs at least 3 next_nodes: "
                "[branch_0, branch_1, ..., receiver]"
            )
        self.branch_nodes = nodes[:-1]
        self.receiver_idx = len(self.branch_nodes)  # index of receiver in next_nodes
        self.dispatch_vars = self.config.get("dispatch_vars", [])
        if len(self.dispatch_vars) != len(self.branch_nodes):
            self.logger.error(
                f"dispatch_vars length ({len(self.dispatch_vars)}) "
                f"!= branch count ({len(self.branch_nodes)})"
            )
        # dispatch_vars reference output_vars TARGET (renamed/wire) names,
        # same rule as dispatch_signals referencing catch targets
        self._target_to_output = {}
        for oname, targets in self.output_dict.items():
            for t in targets:
                self._target_to_output[t] = oname
        for i, names in enumerate(self.dispatch_vars):
            for t in names:
                if t not in self._target_to_output:
                    self.logger.error(
                        f"dispatch_vars[{i}] contains '{t}' which is not an "
                        f"output_vars target of this node"
                    )
        # dispatch_signals[i] = catch-target names directed to branch i
        self.dispatch_signals = self.config.get("dispatch_signals", [])
        # The dispatcher's pass target is the RECEIVER, not the physical next
        # node: take the declarations over from base (so base's generic relay
        # never fires) and handle them in process(). pass-only signals still
        # need to reach process(), so fold them into the catch map unrenamed.
        self.receiver_pass_map = dict(self.pass_signal_map)
        self.pass_signal_map = {}
        self.pass_signal_set = set()
        for src in self.receiver_pass_map:
            self.catch_signal_map.setdefault(src, src)
        self.catch_signal_set = set(self.catch_signal_map)
        catch_targets = set(self.catch_signal_map.values())
        for i, sigs in enumerate(self.dispatch_signals):
            for s in sigs:
                if s not in catch_targets:
                    self.logger.error(
                        f"dispatch_signals[{i}] contains '{s}' which is not "
                        f"a catch_signals target of this node"
                    )
        # Reverse map: catch target -> source (for receiver_pass lookup)
        self._catch_source_of = {t: s for s, t in self.catch_signal_map.items()}

    def process(self, data, pass_data={}):
        # Caught signal (renamed to its catch target by base):
        #   1. direct copies to branches subscribed via dispatch_signals
        #   2. if declared in pass_signals: relay DIRECTED TO THE RECEIVER
        #      (renamed by the pass declaration), transiting branch nodes
        signal = data.get("signal", "")
        if signal != "":
            for i in reversed(range(len(self.branch_nodes))):
                if i < len(self.dispatch_signals) and signal in self.dispatch_signals[i]:
                    self.output_to_queue(
                        {"signal": signal},
                        {"timestamp": pass_data.get("timestamp")},
                        destination_index=i,
                    )
            source = self._catch_source_of.get(signal)
            if source in self.receiver_pass_map:
                self.output_to_queue(
                    {"signal": self.receiver_pass_map[source]},
                    {"timestamp": pass_data.get("timestamp")},
                    destination_index=self.receiver_idx,
                )
            return
        self.logger.info(
            f"dispatching to branches {self.branch_nodes}, "
            f"receiver {self.config['next_nodes'][self.receiver_idx]}"
        )

        ts_only = {"timestamp": pass_data.get("timestamp")}

        # 1. Start signal with pass_data -> receiver
        self.emit_signal(
            "dispatch_start", pass_data,
            destination_index=self.receiver_idx,
        )

        # 2. Branch messages in REVERSE order (later node first).
        # dispatch_vars name the wire (output_vars target) fields.
        for i in reversed(range(len(self.branch_nodes))):
            msg = {}
            for target in self.dispatch_vars[i]:
                oname = self._target_to_output.get(target)
                if oname is not None and oname in data:
                    msg[target] = data[oname]
            self.output_to_queue(msg, ts_only, destination_index=i)

        # 3. End signal -> receiver
        self.emit_signal(
            "dispatch_end", ts_only,
            destination_index=self.receiver_idx,
        )
