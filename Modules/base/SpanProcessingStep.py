import json
import queue

from .BaseProcessingStep import BaseProcessingStep, TIMEOUT


class SpanProcessingStep(BaseProcessingStep):
    """Base class for modules whose processing spans multiple messages.

    A "span" is a period where the module is actively accumulating data
    across multiple messages before producing output. During a span,
    current_timestamp stays set so that cancel can properly interrupt it.

    Span state is tracked via current_timestamp itself:
      - current_timestamp is not None → span active (set by start_span)
      - current_timestamp is None → no active span (set by end_span)

    Subclass API:
      - span_init(): initialization (instead of custom_init)
      - span_process(data, pass_data): handle each message
      - on_span_cancel(): clean up when cancel interrupts a span
      - custom_update(): timer-based logic (same as base)

    Span control (call from span_process / custom_update):
      - start_span(timestamp): set current_timestamp, span begins
      - end_span(): clear current_timestamp, span ends
      - span_active: property, True when current_timestamp is set
    """

    def custom_init(self):
        self.span_init()

    def span_init(self):
        """Subclass initialization. Override instead of custom_init."""
        pass

    def span_process(self, data, pass_data={}):
        """Handle one message. Override in subclass.
        Call start_span() / end_span() to control span lifecycle."""
        pass

    def on_span_cancel(self, cancel_message):
        """Called when cancel interrupts an active span. Override to clean up."""
        pass

    @property
    def span_active(self):
        return self.current_timestamp is not None

    def start_span(self, timestamp):
        """Mark the beginning of a processing span."""
        self.current_timestamp = timestamp

    def end_span(self):
        """Mark the end of a processing span."""
        self.current_timestamp = None

    def custom_cancel(self, cancel_message):
        """Called by check_cancel when current_timestamp < cancel_timestamp."""
        if self.span_active:
            self.on_span_cancel(cancel_message)
            self.end_span()

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
                msg_timestamp = data["timestamp"]
                if msg_timestamp < self.cancel_timestamp:
                    self.logger.info(f"discarding old data: {data}")
                    continue

                # Signal handling: same four-state rules as
                # BaseProcessingStep (catch / catch+pass / pass / undeclared;
                # relaying strips destination — see base)
                signal = data.get("signal", "")
                if signal != "" and signal not in self.catch_signal_set:
                    if signal in self.pass_signal_set:
                        data.pop("destination", None)
                        data["signal"] = self.pass_signal_map[signal]
                        self.output_queue.put(json.dumps(data))
                    else:
                        self.logger.error(
                            f"undeclared signal '{signal}' at node "
                            f"{self.index}; dropping — declare it in "
                            f"catch_signals or pass_signals"
                        )
                    continue

                # Extract data for processing (caught signal renamed)
                if signal != "":
                    filtered_data = {
                        k: v for k, v in data.items()
                        if k not in ("destination",)
                    }
                    filtered_data["signal"] = self.catch_signal_map[signal]
                    pass_data = {"timestamp": data.get("timestamp")}
                else:
                    filtered_data = self.extract_input_data(data)
                    pass_data = self.extract_pass_data(data)
                self.logger.info(f"processing data: {filtered_data}")
                self.span_process(filtered_data, pass_data)

                # Consume-then-relay for caught signals (see base; relaying
                # strips the destination)
                if signal != "" and signal in self.pass_signal_set:
                    relay = {k: v for k, v in data.items()
                             if k != "destination"}
                    relay["signal"] = self.pass_signal_map[signal]
                    self.output_queue.put(json.dumps(relay))

                # Don't reset current_timestamp — span_process manages it
                # via start_span() / end_span()

            except queue.Empty:
                self.custom_update()
            except Exception as e:
                self.logger.error(f"{e}")
