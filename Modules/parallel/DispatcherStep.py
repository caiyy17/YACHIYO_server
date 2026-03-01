"""
Dispatcher node: splits one message into parallel branch messages.

Config:
    next_nodes: [branch_0_id, branch_1_id, ..., receiver_id]
        The last entry is always the receiver node.
    dispatch_vars: [
        ["field_a", "field_b"],   # fields for branch 0
        ["field_c"],              # fields for branch 1
    ]

Emit order:
    1. signal "dispatch_start" + pass_data -> receiver
    2. Branch messages in REVERSE topological order (later branch first)
    3. signal "dispatch_end" -> receiver
"""

from ..base.BaseProcessingStep import BaseProcessingStep


class DispatcherStep(BaseProcessingStep):
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

    def extract_input_data(self, data):
        """Pass all fields through."""
        return dict(data)

    def process(self, data, pass_data={}):
        self.logger.info(
            f"dispatching to branches {self.branch_nodes}, "
            f"receiver {self.config['next_nodes'][self.receiver_idx]}"
        )

        ts_only = {"timestamp": pass_data.get("timestamp")}

        # 1. Start signal with pass_data -> receiver
        self.output_to_queue(
            {"signal": "dispatch_start"}, pass_data,
            destination_index=self.receiver_idx,
        )

        # 2. Branch messages in REVERSE order (later node first)
        for i in reversed(range(len(self.branch_nodes))):
            msg = {}
            for field in self.dispatch_vars[i]:
                if field in data:
                    msg[field] = data[field]
            self.output_to_queue(msg, ts_only, destination_index=i)

        # 3. End signal -> receiver
        self.output_to_queue(
            {"signal": "dispatch_end"}, ts_only,
            destination_index=self.receiver_idx,
        )
