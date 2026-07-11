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
    streams: [                      # any number of streams
        {"caller": "openai_tts",    # caller registry name
         "input":  [{"source": "text", "target": "prompt"}],
         "output": [{"source": "audio", "target": "audio"}],
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

Packing rule: one chunk from each live stream per message, blocking until
every live stream has one (the slowest side sets the pace). When a stream
ends, the remaining streams keep going — one chunk each per message — until
all are done; nothing is dropped, nothing is padded.

Output protocol (single-in-multi-out, same shape as the LLM turn):
    sentence-level SoS  {pass_data: {...}, timestamp}
    pack xN             {<output fields of the still-live streams>, timestamp}
    sentence-level EoS  {timestamp}
Internal envelope names are SoS/EoS; wire names must be renamed in config
when the turn-level SoS/EoS also passes through this node (the emit/pass
wire-name clash check enforces that).

Turn-level SoS (continuous callers): when any stream's sub-config sets
"continuous", this node must catch the turn-level SoS; on catching it every
caller exposing reset_history() is reset.

Cancel: checked between packs; the envelope is NOT closed on cancel (the
whole turn is stale). Pump threads are daemons and drain their callers'
streams to the end — an in-flight backend stream is consumed and discarded,
not aborted.
"""

import queue
import threading

from ..base.BaseProcessingStep import BaseProcessingStep
from ..utils.functions import bytes_to_base64

_DONE = object()


class JointStreamStep(BaseProcessingStep):
    LOG_CONTENT = False  # consumes chunk streams (b64 audio / pose data)
    # Sentence-level stream envelope (this node is inherently streaming)
    EMIT_SIGNALS = ["SoS", "EoS"]

    @classmethod
    def required_inputs(cls, config):
        # every configured stream's input sources must be wired
        return [e.get("source")
                for s in config.get("streams", [])
                for e in (s.get("input") or [])
                if isinstance(e, dict) and e.get("source")]

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
        output_sources = {v.get("source")
                          for v in config.get("output_vars", [])}
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
            for e in s.get("output") or []:
                if (isinstance(e, dict) and e.get("target")
                        and e["target"] not in output_sources):
                    errors.append(
                        f"streams[{i}] output target '{e['target']}' has "
                        f"no output_vars entry"
                    )
        return errors

    def custom_init(self):
        # Callers are resolved by registry name — no cross-module imports;
        # the registry import is deferred so the parallel package import
        # never depends on other modules' dependency chains.
        from .. import get_caller_class_by_name
        self.streams = []
        for s in self.config.get("streams", []):
            caller_cls = get_caller_class_by_name(s["caller"])
            self.streams.append({
                "input": s["input"],
                "output": s["output"],
                "caller": caller_cls(s.get("config", {}), self.logger),
            })

    def custom_cancel(self, cancel_message):
        for s in self.streams:
            if hasattr(s["caller"], "reset_history"):
                s["caller"].reset_history()

    def _pump(self, gen, q):
        """Drain one caller stream into a local queue, then mark it done."""
        try:
            for chunk in gen:
                if chunk:
                    q.put(chunk)
        finally:
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
        """Blocking take with cancel polling; returns _DONE on cancel too."""
        while True:
            if self.check_cancel():
                self.logger.info("cancelled during joint stream")
                return _DONE
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
        start = {"timestamp": pass_data.get("timestamp")}
        wrapped = {k: v for k, v in pass_data.items() if k != "timestamp"}
        if wrapped:
            start["pass_data"] = wrapped
        self.emit_signal("SoS", start)

        # one pump thread + local queue per stream. Inputs are renamed to
        # the caller's parameter names (call_stream(**kwargs)); a stream
        # whose inputs are ALL empty is already done.
        live = []
        for s in self.streams:
            kwargs = {e["target"]: data.get(e["source"], "")
                      for e in s["input"]}
            # whitespace-only counts as empty (e.g. an LLM sentence of bare
            # newlines must not start a TTS stream)
            if all(not str(v).strip() for v in kwargs.values()):
                continue
            q = queue.Queue()
            threading.Thread(
                target=self._pump,
                args=(s["caller"].call_stream(**kwargs), q),
                daemon=True,
            ).start()
            live.append({"output": s["output"], "queue": q})

        while live:
            pack = {}
            for entry in list(live):
                chunk = self._take(entry["queue"])
                if self.check_cancel():
                    return  # no EoS: the whole turn is stale
                if chunk is _DONE:
                    live.remove(entry)
                    continue
                self._pack_chunk(pack, entry["output"], chunk)
            if pack:
                # paired chunks carry b64 audio + pose data — never log
                self.output_to_queue(pack, pass_data,
                                     is_add_pass_data=False, is_log=False)

        self.emit_signal("EoS", {"timestamp": pass_data.get("timestamp")})
        return
