"""
Receiver node: collects parallel branch outputs and merges them.

Uses standard input_vars/output_vars:
    input_vars:  declares expected branch outputs (extract_input_data
                 renames them on each arriving branch message)
    output_vars: maps collected field names to final output names

Catches signals dispatch_start and dispatch_end.
Between start and end, accumulates extracted branch data.
On end:
  - Merged branch data goes through add_output (output_vars mapping)
  - Base layer from start signal serves as pass_data

Messages arriving when not collecting are forwarded unchanged.
FIFO guarantees groups never interleave.
"""

from ..base.SpanProcessingStep import SpanProcessingStep


class ReceiverStep(SpanProcessingStep):
    REQUIRED_CATCH_SIGNALS = ["dispatch_start", "dispatch_end"]
    # pure merger: expected branch fields and their output names are
    # entirely config-defined
    FREE_INPUTS = True
    FREE_OUTPUTS = True

    """Requires config: catch_signals: ["dispatch_start", "dispatch_end"].

    Span module: the collection window (dispatch_start .. dispatch_end)
    is a span — current_timestamp stays set BETWEEN branch messages, so a
    cancel arriving mid-collection triggers the hook and the half-built
    group is dropped at once."""

    def span_init(self):
        self.current_group = None

    def span_process(self, data, pass_data={}):
        signal = data.get("signal", "")

        # Start signal: begin collecting. The group's pass data rides the
        # start signal wrapped under the fixed "pass_data" key; store it as
        # the base layer (renamed via this node's pass_vars when declared).
        if signal == "dispatch_start":
            carried = data.get("pass_data", {})
            pass_vars = self.config.get("pass_vars", [])
            if pass_vars:
                base = {pv["target"]: carried[pv["source"]]
                        for pv in pass_vars if pv["source"] in carried}
            else:
                base = dict(carried)
            self.current_group = {"base": base, "branches": []}
            self.start_span(data["timestamp"])
            self.logger.info("dispatch_start")
            return

        # End signal: output merged branch results with base as pass_data
        if signal == "dispatch_end":
            if self.current_group is not None:
                n = len(self.current_group["branches"])
                self.logger.info(f"dispatch_end, merging {n} branches")

                # Merge all branch data (already renamed by extract_input_data)
                merged = {}
                for branch_data in self.current_group["branches"]:
                    merged.update(branch_data)

                # Apply output_vars mapping
                output_data = {}
                for key, value in merged.items():
                    self.add_output(output_data, key, value)

                # Base layer (from start signal) as pass_data
                base_pass = dict(self.current_group["base"])
                base_pass["timestamp"] = pass_data.get("timestamp")

                self.current_group = None
                self.output_to_queue(output_data, base_pass)
                self.end_span()
            return

        # Collecting: accumulate branch output
        # (normal messages go through extract_input_data in base run loop,
        #  so field names are already renamed via input_vars)
        if self.current_group is not None:
            branch_data = {
                k: v for k, v in data.items()
                if k not in ("signal", "timestamp", "destination")
            }
            self.current_group["branches"].append(branch_data)
            self.logger.info(
                f"received branch (total: {len(self.current_group['branches'])})"
            )
            return

        # Not collecting: forward unchanged with output_vars mapping
        output_data = {}
        for key, value in data.items():
            if key not in ("signal", "timestamp", "destination"):
                self.add_output(output_data, key, value)
        self.output_to_queue(output_data, pass_data)

    def on_span_cancel(self, cancel_message):
        self.current_group = None

    def custom_dispose(self):
        self.current_group = None
