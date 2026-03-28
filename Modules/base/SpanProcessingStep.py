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
                dest = data.get("destination", self.index)
                if dest != self.index and dest != -2:
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

                # If this is a signal not in catch_signal_set, forward
                if (
                    data.get("signal", "") != ""
                    and data.get("signal", "") not in self.catch_signal_set
                ):
                    data.pop("destination", None)
                    self.output_queue.put(json.dumps(data))
                    continue

                # Extract data for processing
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
                self.span_process(filtered_data, pass_data)

                # Don't reset current_timestamp — span_process manages it
                # via start_span() / end_span()

            except queue.Empty:
                self.custom_update()
            except Exception as e:
                self.logger.error(f"{e}")
