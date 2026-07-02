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
        self.catch_signal_set = set()
        self.prepare_output_dict()
        self._init_with_timeout()
        self.logger.info("initialized")

    def _init_with_timeout(self, timeout=60):
        """Run custom_init with a timeout to prevent hanging on unreachable services."""
        import threading
        init_done = threading.Event()
        init_error = [None]

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
            self.logger.error(f"custom_init timed out after {timeout}s")
        elif init_error[0]:
            self.logger.error(f"custom_init failed: {init_error[0]}")

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

                # If this is a signal not in catch_signal_set, remove destination and forward to output_queue
                if (
                    data.get("signal", "") != ""
                    and data.get("signal", "") not in self.catch_signal_set
                ):
                    data.pop("destination", None)
                    self.output_queue.put(json.dumps(data))
                    self.current_timestamp = None
                    continue

                # Caught signals: pass all fields directly (no filtering)
                # Normal messages: filter through input_vars/pass_vars
                if data.get("signal", "") in self.catch_signal_set:
                    filtered_data = {
                        k: v for k, v in data.items()
                        if k not in ("destination",)
                    }
                    pass_data = {"timestamp": data.get("timestamp")}
                else:
                    filtered_data = self.extract_input_data(data)
                    pass_data = self.extract_pass_data(data)
                self.logger.info(f"processing data: {filtered_data}")
                self.process(filtered_data, pass_data)
                self.current_timestamp = None
            except queue.Empty:
                self.custom_update()
            except Exception as e:
                self.logger.error(f"{e}")

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
