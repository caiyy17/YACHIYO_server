from collections import deque
from fractions import Fraction

from .SpanProcessingStep import SpanProcessingStep


class ChunkGenerationCancelled(Exception):
    """Internal cooperative-cancellation marker; never emitted on the wire."""


class ChunkGenerationSession:
    """One SoS..EoS generation session.

    finish() and abort() must completely release span-local state. A caller
    may reuse the object (and a transport) across spans; close() is then the
    node-lifetime cleanup invoked by its concrete generation_dispose().

    A remote-backed session may set pipelined = True and implement the
    submit/poll/has_pending/next_result quartet instead of a blocking
    generate_chunk: the step then fires one request per input without
    waiting for its reply, emits replies as they return (FIFO, replies
    carry no ids), and drains the in-flight tail at stream_end. abort()
    must discard everything in flight.
    """

    passthrough = False
    pipelined = False

    def generate_chunk(self, inputs, chunk_index):
        raise NotImplementedError

    def submit(self, inputs, chunk_index):
        """Pipelined only: fire one chunk request without waiting."""
        raise NotImplementedError

    def poll(self):
        """Pipelined only: already-arrived results, oldest first."""
        raise NotImplementedError

    def has_pending(self):
        """Pipelined only: True while any request is still in flight."""
        return False

    def next_result(self):
        """Pipelined only: block until the oldest in-flight result returns."""
        raise NotImplementedError

    def finish(self):
        pass

    def abort(self):
        pass

    def close(self):
        pass


class PassthroughChunkGenerationSession(ChunkGenerationSession):
    """A span that relays each configured pass-through payload unchanged."""

    passthrough = True


def frames_for_duration(duration_ms, framerate):
    """Return the exact integral frame count for a timed chunk."""
    frames = Fraction(str(duration_ms)) * Fraction(str(framerate)) / 1000
    if frames <= 0 or frames.denominator != 1:
        raise ValueError(
            f"chunk_duration_ms * framerate / 1000 must be a positive "
            f"integer, got {duration_ms}ms * {framerate}fps = {float(frames)}"
        )
    return frames.numerator


class ChunkGenerationStep(SpanProcessingStep):
    """Stateful one-input-packet -> one-fixed-duration-chunk transform.

    The step consumes and passes through one configured stream envelope. It
    owns no SoS/EoS names of its own: catch+pass ordering opens the generation
    session before SoS is relayed and finishes it before EoS is relayed.

    All configured input_vars and pass_vars are supplied to the session.
    Fields that must survive downstream still need an explicit pass_vars
    declaration, following the normal pipeline contract.
    """

    REQUIRED_CATCH_SIGNALS = ["stream_start", "stream_end"]
    FREE_INPUTS = True

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)

        # This transform doesn't emit a replacement envelope, so both owning
        # boundary signals must be real wire signals and must be relayed.
        caught = {}
        for entry in config.get("catch_signals") or []:
            if isinstance(entry, dict):
                caught[entry.get("target")] = entry.get("source")
        passed = {
            entry.get("source")
            for entry in config.get("pass_signals") or []
            if isinstance(entry, dict)
        }
        for internal in cls.REQUIRED_CATCH_SIGNALS:
            source = caught.get(internal)
            if source is None:
                errors.append(
                    f"'{internal}' must catch a non-null wire signal"
                )
            elif source not in passed:
                errors.append(
                    f"wire signal '{source}' caught as '{internal}' must "
                    f"also be declared in pass_signals"
                )
        return errors

    def span_init(self):
        self._generation_session = None
        self._span_context = {}
        self._chunk_index = 0
        # Pipelined sessions: per-request pass-through context, so a reply
        # emits with ITS OWN meta/timestamp (FIFO, same order as requests).
        self._pending = deque()
        self.generation_init()

    def generation_init(self):
        """One-time module initialization hook."""
        pass

    def open_generation_session(self, start_context):
        """Create or reset and return the session for a new span."""
        raise NotImplementedError

    def generation_dispose(self):
        """One-time module cleanup hook."""
        pass

    def _start_context(self, data):
        carried = data.get("pass_data") or {}
        if not isinstance(carried, dict):
            raise TypeError("stream start pass_data must be an object")
        context = dict(carried)
        for mapping in self.config.get("pass_vars") or []:
            source, target = mapping["source"], mapping["target"]
            if source in carried:
                context[target] = carried[source]
        context.update({
            key: value for key, value in data.items()
            if key not in ("signal", "pass_data", "destination")
        })
        return context

    def span_process(self, data, pass_data=None):
        pass_data = pass_data or {}
        signal = data.get("signal", "")
        if signal == "stream_start":
            self._start_generation(data)
            return
        if signal == "stream_end":
            if not self.span_active or self._generation_session is None:
                self.logger.warning(
                    "stream_end without an active stream; internal no-op"
                )
                return
            self._finish_generation()
            return

        if not self.span_active or self._generation_session is None:
            self.logger.warning("chunk outside an active stream; dropped")
            return
        if self.check_cancel():
            return
        if getattr(self._generation_session, "passthrough", False):
            self.output_to_queue({}, pass_data, log_level=0)
            self._chunk_index += 1
            return

        # Span context is the stable layer, then current pass/input fields
        # override it. input_vars therefore remain the explicit model-facing
        # interface while pass_vars can also be used as generation context.
        inputs = dict(self._span_context)
        inputs.update(pass_data)
        inputs.update(data)
        session = self._generation_session
        if getattr(session, "pipelined", False):
            # Fire-and-continue: the reply for this request emits on a
            # later input (or at stream_end), paired FIFO with the context
            # queued here. Nothing blocks, so no cancel window opens.
            session.submit(inputs, self._chunk_index)
            self._pending.append(dict(pass_data))
            self._chunk_index += 1
            for result in session.poll():
                self._emit_result(result, self._pending.popleft())
            return
        try:
            result = session.generate_chunk(inputs, self._chunk_index)
        except ChunkGenerationCancelled:
            self._abort_generation()
            self.end_span()
            return

        # A cancel may have arrived while a blocking generator was working.
        if self.check_cancel() or not self.span_active:
            return
        self._emit_result(result, pass_data)
        self._chunk_index += 1

    def _emit_result(self, result, pass_data):
        if not isinstance(result, dict) or not result:
            raise ValueError("generate_chunk() must return a non-empty object")
        expected = set(self.module_outputs(self.config))
        if expected and not expected.intersection(result):
            raise ValueError(
                f"generate_chunk() returned none of {sorted(expected)}"
            )
        output = {}
        for key, value in result.items():
            self.add_output(output, key, value)
        self.output_to_queue(output, pass_data, log_level=0)

    def _start_generation(self, data):
        # A repeated owning SoS is a reset boundary. Drop the unfinished old
        # span without inventing an EoS, then start cleanly from this signal.
        if self.span_active or self._generation_session is not None:
            self.logger.warning("stream_start while active; resetting session")
            self._abort_generation()
            self.end_span()

        context = self._start_context(data)
        timestamp = context.get("timestamp")
        if timestamp is None:
            raise ValueError("stream_start needs a timestamp")

        self._span_context = context
        self._chunk_index = 0
        self.start_span(timestamp)
        session = self.open_generation_session(dict(context))
        if session is None:
            raise RuntimeError("open_generation_session() returned None")
        self._generation_session = session

    def _finish_generation(self):
        session = self._generation_session
        if session is not None and getattr(session, "pipelined", False):
            # Chunk-count contract: every submitted request must come back
            # before the envelope closes. A cancel arriving during this
            # wait aborts the span; in-flight replies are discarded.
            try:
                while session.has_pending():
                    self._emit_result(
                        session.next_result(), self._pending.popleft()
                    )
            except ChunkGenerationCancelled:
                self._abort_generation()
                self.end_span()
                return
        if session is not None:
            # Keep it attached until finish succeeds. If finish raises, the
            # SpanProcessingStep error boundary can still abort it.
            session.finish()
        self._generation_session = None
        self._span_context = {}
        self._chunk_index = 0
        self._pending.clear()
        self.end_span()

    def _abort_generation(self):
        session = self._generation_session
        self._generation_session = None
        self._span_context = {}
        self._chunk_index = 0
        self._pending.clear()
        if session is None:
            return
        try:
            session.abort()
        except Exception as error:
            self.logger.error(
                f"generation abort failed: {type(error).__name__}: {error}"
            )

    def on_span_cancel(self, cancel_message):
        self._abort_generation()

    def custom_dispose(self):
        self._abort_generation()
        self.end_span()
        self.generation_dispose()
