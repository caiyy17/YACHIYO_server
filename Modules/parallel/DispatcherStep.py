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
        ["other_field"],     # branch 1 fields
    ]
    dispatch_signals: [      # the signal-side parallel of dispatch_vars:
        ["SoS", "EoS"],      # which CAUGHT signals (by catch TARGET name)
        ["SoS"],             # each branch receives, as directed copies.
    ]                        # The same name may appear in several lists.

Signal semantics (the dispatcher's logical mainline is the RECEIVER):
    catch_signals -> a signal must be CAUGHT to be dispatched: catch is the
                     entry (renamable as usual), dispatch_signals reference
                     the caught (target) names. The validator enforces the
                     exact match both ways: every catch target referenced by
                     some branch list, every reference a real catch target.
    pass_signals  -> relayed DIRECTED TO THE RECEIVER (renamed), transiting
                     the branch nodes via destination routing — branches
                     neither see nor declare them. Implemented by overriding
                     the base _relay_signal hook. The receiver declares its
                     own pass to continue the signal downstream.

Emit order:
    1. signal "dispatch_start" + pass_data -> receiver
    2. Branch messages in REVERSE topological order (later branch first)
    3. signal "dispatch_end" -> receiver
"""

import json

from ..base.BaseProcessingStep import BaseProcessingStep


class DispatcherStep(BaseProcessingStep):
    # Directed group envelope to the receiver. Renaming these via config
    # emit_signals (and catching the renamed pair at the inner receiver) is
    # what makes NESTED dispatcher/receiver brackets wireable without the
    # inner envelope colliding with the outer one.
    EMIT_SIGNALS = ["dispatch_start", "dispatch_end"]
    # pure router: its whole data interface is config-defined (input_vars
    # feed output_vars, dispatch_vars slice them per branch)
    FREE_INPUTS = True
    FREE_OUTPUTS = True

    @classmethod
    def validate_config(cls, config):
        """Dispatcher structure on top of the base checks: next_nodes shape,
        dispatch_vars referencing output targets, dispatch_signals
        referencing catch targets exactly both ways (a signal must be caught
        to be dispatched; an unreferenced catch would be swallowed)."""
        errors = super().validate_config(config)
        nn = config.get("next_nodes", [])
        if len(nn) < 3:
            errors.append(
                "dispatcher needs >=3 next_nodes [branch..., receiver]")
        n_branches = max(len(nn) - 1, 0)
        dv = config.get("dispatch_vars", [])
        if len(dv) != n_branches:
            errors.append(
                f"dispatch_vars length {len(dv)} != branch count {n_branches}"
            )
        output_targets = {v.get("target")
                          for v in config.get("output_vars", [])}
        for bi, names in enumerate(dv):
            for t in names:
                if t not in output_targets:
                    errors.append(
                        f"dispatch_vars[{bi}] field '{t}' is not an "
                        f"output_vars target"
                    )
        ds = config.get("dispatch_signals", [])
        if ds and len(ds) > n_branches:
            errors.append(
                f"dispatch_signals has {len(ds)} entries for "
                f"{n_branches} branches"
            )
        catch_targets = {e["target"] for e in config.get("catch_signals") or []
                         if isinstance(e, dict) and e.get("target")}
        referenced = set()
        for bi, sigs in enumerate(ds):
            seen = set()
            for s in sigs:
                if s in seen:
                    errors.append(
                        f"dispatch_signals[{bi}] lists '{s}' more than once")
                seen.add(s)
                referenced.add(s)
                if s not in catch_targets:
                    errors.append(
                        f"dispatch_signals[{bi}] signal '{s}' is not a "
                        f"catch_signals target — a signal must be caught to "
                        f"be dispatched"
                    )
        for t in sorted(catch_targets - referenced):
            errors.append(
                f"catch_signals target '{t}' is not referenced by any "
                f"dispatch_signals branch — it would be silently swallowed"
            )
        return errors

    @classmethod
    def _validate_catch_contract(cls, config, catch_targets, errors):
        # The dispatcher consumes nothing itself: its catch contract is the
        # set of names referenced by dispatch_signals, checked exactly both
        # ways in validate_config above — the base required==targets rule
        # does not apply.
        pass

    def custom_init(self):
        nodes = self.config.get("next_nodes", [])
        self.branch_nodes = nodes[:-1]
        self.receiver_idx = len(self.branch_nodes)  # index of receiver in next_nodes
        self.dispatch_vars = self.config.get("dispatch_vars", [])
        # dispatch_vars reference output_vars TARGET (renamed/wire) names
        self._target_to_output = {}
        for oname, targets in self.output_dict.items():
            for t in targets:
                self._target_to_output[t] = oname
        # dispatch_signals[i] = caught signal names (catch TARGETS) branch i
        # receives (validated statically by validate_config)
        self.dispatch_signals = self.config.get("dispatch_signals", [])

    def _relay_signal(self, relay):
        """The dispatcher's pass relay is DIRECTED TO THE RECEIVER (its
        logical mainline): the copy transits the branch nodes via destination
        routing, so branches neither see nor declare it."""
        self.add_destination(relay, self.receiver_idx)
        self.output_queue.put(json.dumps(relay))

    def process(self, data, pass_data={}):
        # Caught signal (renamed to its catch target by base): directed
        # copies to the branches subscribed via dispatch_signals, in REVERSE
        # branch order like data dispatch. (Relaying to the receiver is
        # handled by _relay_signal.)
        signal = data.get("signal", "")
        if signal != "":
            for i in reversed(range(len(self.branch_nodes))):
                if i < len(self.dispatch_signals) and signal in self.dispatch_signals[i]:
                    self.output_to_queue(
                        {"signal": signal},
                        {"timestamp": pass_data.get("timestamp")},
                        destination_index=i,
                    )
            return
        self.logger.info(
            f"dispatching to branches {self.branch_nodes}, "
            f"receiver {self.config['next_nodes'][self.receiver_idx]}"
        )

        # 1. Start signal -> receiver, carrying the group's pass_vars data
        # wrapped under the fixed "pass_data" key (shape built here by the
        # caller; the receiver reads its base layer from that key)
        start = {"timestamp": pass_data.get("timestamp")}
        wrapped = {k: v for k, v in pass_data.items() if k != "timestamp"}
        if wrapped:
            start["pass_data"] = wrapped
        self.emit_signal(
            "dispatch_start", start,
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
            self.output_to_queue(msg, pass_data, destination_index=i,
                                 is_add_pass_data=False)

        # 3. End signal -> receiver
        self.emit_signal(
            "dispatch_end", {"timestamp": pass_data.get("timestamp")},
            destination_index=self.receiver_idx,
        )
