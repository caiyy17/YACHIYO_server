"""
Joint stream node: merges N chunk streams inside ONE node, packing them
chunk by chunk.

Why a single node: the linear pipeline cannot interleave message-level
streams (a chunk crossing another busy node waits for that node's whole
sentence), so the merge happens at the CALLER level, in this node's memory:
the configured callers stream concurrently, the node packs one chunk from
each into one message. Topologically it stays a plain single-in single-out
node.

The ONLY assumption: all streams produce chunks of EQUAL duration — that is
each stream's sub-config's responsibility (e.g. a TTS caller with
stream_chunk_ms: 300 next to a motion caller with stream_frames: 9 @30fps).
This node has no window configuration and no knowledge of what the streams
are; callers are resolved BY NAME from the caller registry (each module
registers its callers in its own __init__ via `caller_map` — no
cross-module imports).

Config:
    mode: "longest"                 # "longest" (default) / "shortest" /
                                      # "anchor"
    anchor: "motion"                # mode=anchor: an output target naming
                                      # the reference stream
    streams: [                      # any number of streams
        {"caller": "openai_tts",    # caller registry name
         "input":  [{"source": "text", "target": "prompt"}],
         "output": [{"source": "audio", "target": "audio"}],
         "extend": true,            # default false; repeat its last chunk
                                      # after it ends
         "config": { ... }},        # sub-config handed verbatim to the caller
        {"caller": "motion_generation",
         "input":  [{"source": "prompt", "target": "prompt"}],
         "output": [{"source": "motion", "target": "motion"}],
         "config": { ... }},
    ]
    input / output are RENAME LISTS (same {source, target} shape as the var
    declarations) because different streams may use the same INTERNAL names:
      input:  source = this node's input field (an input_vars target),
              target = the caller's call_stream() parameter name — the
              caller is invoked as call_stream(**{target: value, ...}), so
              multi-input callers (e.g. prompt/language/speaker) just take
              more entries.
      output: target = an output_vars source for the chunk field,
              source = the caller-side name of that product. Every caller
              yields dict chunks keyed by its product names (one uniform
              shape, however many products); each entry picks chunk[source]
              into target.
    input_vars / output_vars then wire those node-side names as usual
    (required inputs are derived from the streams' input sources).

Packing rule: one chunk from each running stream per message, blocking until
every running stream has one (the slowest side sets the pace). Overall end is
chosen with the same modes as PadStep: "longest" waits for all started
streams, "shortest" stops at the first stream end, and "anchor" stops when
the stream identified by `anchor` ends. `anchor` names any output target of
that stream (e.g. "motion"). A stream ending before the overall boundary is
omitted by default; with per-stream `extend: true`, its exact last chunk is
repeated in subsequent packs. No chunk-duration inspection is performed.

Output protocol (single-in-multi-out, same shape as the LLM turn):
    sentence-level SoS  {pass_data: {...}, timestamp}
    pack xN             {<output fields of running/extended streams>, timestamp}
    sentence-level EoS  {timestamp}
Internal envelope names are SoS/EoS; wire names must be renamed in config
when the turn-level SoS/EoS also passes through this node (the emit/pass
wire-name clash check enforces that).

Turn-level SoS (continuous callers): when any stream's sub-config sets
"continuous", this node must catch the turn-level SoS; on catching it every
caller exposing reset_history() is reset.

Cancel: checked while waiting for chunks and between packing steps; the
envelope is NOT closed on cancel (the whole turn is stale). A per-input stop
event tells every daemon pump to stop at its next iterator boundary, where the
pump closes its generator from the same thread. A caller currently blocked in
next() remains bounded by that caller's next event / read timeout.
"""

import queue
import threading

from ..base.BaseProcessingStep import BaseProcessingStep
from ..utils.functions import bytes_to_base64

_DONE = object()
_CANCELLED = object()


class JointStreamStep(BaseProcessingStep):
    # Sentence-level stream envelope (this node is inherently streaming)
    EMIT_SIGNALS = ["SoS", "EoS"]
    MODES = ("longest", "shortest", "anchor")

    @classmethod
    def required_inputs(cls, config):
        # every configured stream's input sources must be declared
        names = [e.get("source")
                 for s in config.get("streams", [])
                 for e in (s.get("input") or [])
                 if isinstance(e, dict) and e.get("source")]
        return list(dict.fromkeys(names))

    @classmethod
    def module_outputs(cls, config):
        # every configured stream's output targets are this node's products
        names = [e.get("target")
                 for s in config.get("streams", [])
                 for e in (s.get("output") or [])
                 if isinstance(e, dict) and e.get("target")]
        return list(dict.fromkeys(names))

    @classmethod
    def required_catch_signals(cls, config):
        # any continuous caller needs the turn-level SoS to reset history
        if any((s.get("config") or {}).get("continuous")
               for s in config.get("streams", [])):
            return ["SoS"]
        return []

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        streams = config.get("streams", [])
        if not isinstance(streams, list) or not streams:
            errors.append("joint stream needs a non-empty 'streams' list")
            return errors
        from .. import get_caller_class_by_name
        for i, s in enumerate(streams):
            for key in ("caller", "input", "output", "config"):
                if not s.get(key):
                    errors.append(f"streams[{i}] is missing '{key}'")
            name = s.get("caller")
            if name and get_caller_class_by_name(name) is None:
                errors.append(
                    f"streams[{i}] caller '{name}' is not registered"
                )
            for key in ("input", "output"):
                entries = s.get(key)
                if entries is None:
                    continue
                if not isinstance(entries, list) or not entries:
                    errors.append(
                        f"streams[{i}] {key} must be a non-empty "
                        f'[{{"source", "target"}}] rename list'
                    )
                    continue
                for e in entries:
                    if (not isinstance(e, dict)
                            or not e.get("source") or not e.get("target")):
                        errors.append(
                            f"streams[{i}] {key} entry {e!r} is not an "
                            f'explicit {{"source", "target"}} object'
                        )
            if "extend" in s and not isinstance(s["extend"], bool):
                errors.append(
                    f"streams[{i}] extend must be boolean, "
                    f"got {s['extend']!r}"
                )
            subconfig = s.get("config")
            if isinstance(subconfig, dict) \
                    and "exact_chunk" in subconfig \
                    and not isinstance(subconfig["exact_chunk"], bool):
                errors.append(
                    f"streams[{i}].config exact_chunk must be boolean, "
                    f"got {subconfig['exact_chunk']!r}"
                )

        mode = config.get("mode", "longest")
        if mode not in cls.MODES:
            errors.append(
                f"mode must be one of {list(cls.MODES)}, got {mode!r}"
            )
        if mode == "anchor":
            anchor = config.get("anchor")
            matches = [
                i for i, s in enumerate(streams)
                if isinstance(s, dict) and any(
                    isinstance(e, dict) and e.get("target") == anchor
                    for e in (s.get("output") or [])
                )
            ]
            available = sorted({
                e.get("target")
                for s in streams if isinstance(s, dict)
                for e in (s.get("output") or []) if isinstance(e, dict)
                if e.get("target")
            })
            if len(matches) != 1:
                errors.append(
                    f"mode 'anchor' needs an 'anchor' naming exactly one "
                    f"stream output target ({available or 'none'}), "
                    f"got {anchor!r}"
                )
        return errors

    def custom_init(self):
        # Callers are resolved by registry name — no cross-module imports;
        # the registry import is deferred so the parallel package import
        # never depends on other modules' dependency chains.
        from .. import get_caller_class_by_name
        self._active_stop_event = None
        self.mode = self.get_config("mode", "longest")
        self.anchor = self.get_config("anchor", None)
        self.streams = []
        for s in self.config.get("streams", []):
            caller_cls = get_caller_class_by_name(s["caller"])
            self.streams.append({
                "input": s["input"],
                "output": s["output"],
                "extend": s.get("extend", False),
                "is_anchor": any(
                    e["target"] == self.anchor for e in s["output"]
                ),
                "caller": caller_cls(s.get("config", {}), self.logger),
            })

    def custom_cancel(self, cancel_message):
        # One event is shared by every pump belonging to the current input.
        # The node thread returns immediately; a pump that is currently inside
        # a blocking next(gen) stops at the next yielded chunk (or the caller's
        # read timeout), then closes its generator in its own thread.
        stop_event = self._active_stop_event
        if stop_event is not None:
            stop_event.set()
        for s in self.streams:
            if hasattr(s["caller"], "reset_history"):
                s["caller"].reset_history()

    def _pump(self, gen, q, stop_event):
        """Drain one caller stream until completion or cooperative cancel.

        Generators must be closed by the thread currently advancing them;
        calling close() from the node thread while next() is running would
        raise ``ValueError: generator already executing``.  A blocking caller
        therefore observes cancellation at its next iterator boundary.
        """
        try:
            iterator = iter(gen)
            while not stop_event.is_set():
                try:
                    chunk = next(iterator)
                except StopIteration:
                    break
                if stop_event.is_set():
                    break
                if chunk:
                    q.put(chunk)
        finally:
            close = getattr(gen, "close", None)
            if close is not None:
                try:
                    close()
                except Exception as e:
                    self.logger.warning(
                        f"failed to close joint stream generator: "
                        f"{type(e).__name__}: {e}"
                    )
            q.put(_DONE)

    def _pack_chunk(self, pack, outputs, chunk):
        """Place one chunk into the pack via the stream's output rename list.
        Chunks are dicts keyed by the caller's product names; each entry
        picks chunk[source] into pack[target]."""
        if not isinstance(chunk, dict):
            self.logger.error(
                f"non-dict stream chunk dropped: {type(chunk).__name__}"
            )
            return
        for e in outputs:
            if e["source"] not in chunk:
                continue
            value = chunk[e["source"]]
            if isinstance(value, bytes):
                value = bytes_to_base64(value)
            self.add_output(pack, e["target"], value)

    def _take(self, q):
        """Blocking take with cancel polling.

        Cancellation is distinct from a caller's natural _DONE sentinel so
        the outer loop cannot accidentally remove one stream and continue the
        others (or emit EoS) after the turn became stale.
        """
        while True:
            if self.check_cancel():
                self.logger.info("cancelled during joint stream")
                return _CANCELLED
            try:
                return q.get(timeout=0.1)
            except queue.Empty:
                continue

    def process(self, data, pass_data={}):
        if data.get("signal", "") == "SoS":
            for s in self.streams:
                if hasattr(s["caller"], "reset_history"):
                    s["caller"].reset_history()
            return

        # sentence-level SoS carries the per-sentence pass_vars data wrapped
        # under "pass_data" (shape built here; emit_signal ships it flat)
        start = self.envelope(self.stamp({}, pass_data), pass_data, wrap=True)
        self.emit_signal("SoS", start)

        # one pump thread + local queue per stream. Inputs are renamed to
        # the caller's parameter names (call_stream(**kwargs)); a stream
        # whose inputs are ALL empty is already done.
        stop_event = threading.Event()
        self._active_stop_event = stop_event
        try:
            states = []
            for s in self.streams:
                kwargs = {e["target"]: data.get(e["source"], "")
                          for e in s["input"]}
                # whitespace-only counts as empty (e.g. an LLM sentence of
                # bare newlines must not start a TTS stream)
                if all(not str(v).strip() for v in kwargs.values()):
                    continue
                q = queue.Queue()
                threading.Thread(
                    target=self._pump,
                    args=(s["caller"].call_stream(**kwargs), q, stop_event),
                    daemon=True,
                ).start()
                states.append({
                    "output": s["output"],
                    "queue": q,
                    "extend": s["extend"],
                    "is_anchor": s["is_anchor"],
                    "last_chunk": None,
                    "done": False,
                })

            # An anchor stream can be configured correctly but be absent from
            # this particular input because all its arguments are empty. Like
            # PadStep's missing/unreadable anchor, preserve available content
            # rather than truncating it to zero: fall back to longest for this
            # input only.
            mode = self.mode
            if mode == "anchor" and states \
                    and not any(s["is_anchor"] for s in states):
                self.logger.warning(
                    f"anchor '{self.anchor}' has no active stream; "
                    f"falling back to longest for this input"
                )
                mode = "longest"

            end_reached = False
            while any(not entry["done"] for entry in states):
                pack = {}
                produced = False
                for entry in states:
                    if entry["done"]:
                        if entry["extend"] \
                                and entry["last_chunk"] is not None:
                            self._pack_chunk(
                                pack, entry["output"], entry["last_chunk"]
                            )
                        continue

                    chunk = self._take(entry["queue"])
                    if chunk is _CANCELLED:
                        return  # no EoS: the whole turn is stale
                    # Catch a cancel arriving after q.get() returned but before
                    # this chunk is packed. custom_cancel sets stop_event.
                    if self.check_cancel():
                        return  # no EoS: the whole turn is stale
                    if chunk is _DONE:
                        entry["done"] = True
                        if mode == "shortest" \
                                or (mode == "anchor"
                                    and entry["is_anchor"]):
                            end_reached = True
                            break
                        if entry["extend"] \
                                and entry["last_chunk"] is not None:
                            self._pack_chunk(
                                pack, entry["output"], entry["last_chunk"]
                            )
                        continue
                    entry["last_chunk"] = chunk
                    produced = True
                    self._pack_chunk(pack, entry["output"], chunk)

                if end_reached:
                    # Streams beyond the selected boundary are no longer
                    # needed. Their pumps stop cooperatively at the next
                    # iterator boundary; this is natural completion, so the
                    # sentence still closes with EoS below.
                    stop_event.set()
                    break
                # When the last running streams all report _DONE in one
                # round, `pack` may contain only repeated tails. Do not emit
                # an extra filler-only chunk beyond the selected boundary.
                if produced and pack:
                    # paired chunks carry b64 audio + pose data — never log
                    self.output_to_queue(pack, pass_data,
                                         is_add_pass_data=False, log_level=0)

            # A cancel can arrive between consuming the final _DONE and
            # closing the sentence. Give it one last checkpoint so only a
            # genuinely natural completion emits EoS.
            if self.check_cancel():
                return
            self.emit_signal("EoS", self.stamp({}, pass_data))
            return
        finally:
            # Also stops pumps if packing itself raises. Natural completion
            # has already exhausted them, so setting the event is harmless.
            stop_event.set()
            if self._active_stop_event is stop_event:
                self._active_stop_event = None
