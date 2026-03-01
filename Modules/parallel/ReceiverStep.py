"""
Receiver node: collects parallel branch outputs and merges them.

Catches signals dispatch_start and dispatch_end.
Between start and end, accumulates all arriving data as branch outputs.
On end:
  - Branch outputs go through add_output (output_vars mapping applied)
  - Base layer from start signal serves as pass_data (forwarded via add_pass_data)

Messages arriving when not collecting are forwarded unchanged.
FIFO guarantees groups never interleave.
"""

from ..base.BaseProcessingStep import BaseProcessingStep


class ReceiverStep(BaseProcessingStep):
    def custom_init(self):
        self.catch_signal_set = {"dispatch_start", "dispatch_end"}
        self.current_group = None

    def extract_input_data(self, data):
        """Pass all fields through."""
        return dict(data)

    def process(self, data, pass_data={}):
        signal = data.get("signal", "")

        # Start signal: begin collecting, store pass_data as base layer
        if signal == "dispatch_start":
            base = {
                k: v for k, v in data.items()
                if k not in ("signal", "timestamp", "destination")
            }
            self.current_group = {"base": base, "branches": []}
            self.logger.info("dispatch_start")
            return

        # End signal: output branch results with base as pass_data
        if signal == "dispatch_end":
            if self.current_group is not None:
                n = len(self.current_group["branches"])
                self.logger.info(f"dispatch_end, merging {n} branches")

                # Branch outputs through add_output (applies output_vars mapping)
                output_data = {}
                for branch_data in self.current_group["branches"]:
                    for key, value in branch_data.items():
                        self.add_output(output_data, key, value)

                # Base layer (from start signal) as pass_data
                base_pass = dict(self.current_group["base"])
                base_pass["timestamp"] = pass_data.get("timestamp")

                self.current_group = None
                self.output_to_queue(output_data, base_pass)
            return

        # Collecting: accumulate branch output
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

        # Not collecting: forward unchanged
        output_data = {}
        for key, value in data.items():
            if key not in ("signal", "timestamp", "destination"):
                self.add_output(output_data, key, value)
        self.output_to_queue(output_data, pass_data)

    def custom_cancel(self, cancel_message):
        self.current_group = None

    def custom_dispose(self):
        self.current_group = None
