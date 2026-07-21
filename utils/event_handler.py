import json
import threading
from queue import Queue

# Numerical guard applied to every dispatched cancel stamp: producers submit
# their SEMANTIC stamp (the turn's own timestamp); nudging it down by a hair
# on dispatch makes the strict < comparison reliably spare same-stamp
# messages (the very turn a barge-in opens) across float round-trips. This
# is arithmetic hygiene, not a cancel offset.
CANCEL_EPSILON = 1e-3

class EventHandler:
    """Per-pipeline event processor: the control plane as a message consumer.

    The pipeline builds the per-node control queues (alongside the data
    queues) and hands them in as {node_id: queue}; nodes hold the consuming
    end of their own queue, and this class is the single writer into them.

    Producers never touch dispatch logic — they submit() event messages
    into the handler's inbox. Every event carries a `source`: node ids for
    pipeline emitters (e.g. a model-driven VAD detecting speech onset
    submits its barge-in cancel with source = its node id), and 0 for the
    pipeline boundary — client cancels (the entry FORCES 0, so a client
    cannot impersonate a node) and lifecycle teardown. Absent still means
    0, as a default only.
    A dedicated thread consumes events in order and applies the policy:

      cancel — broadcast to every node's control queue EXCEPT the source
               (the emitter has already handled its own state). A
               server-originated cancel (source != 0) additionally goes out
               through the send queue: the client's network module hands it
               to the client-side event handler, which mirrors the same
               dispatch (everyone but the receiving module). A client
               cancel is never echoed back.
      kill   — broadcast to every node's control queue, then the handler
               itself exits (it dies last, right after delivering the verb)
      other  — a config-declared event verb (the entry admits only the
               pipeline's top-level `events` list): plain broadcast, same
               source exclusion as cancel, no stamp adjustment, no client
               echo. Nodes that declare it in catch_events consume it;
               the rest skip it.

    The thread is a daemon: its only exit is the kill event, and it must
    never block interpreter shutdown.
    """

    def __init__(self, node_queues, send_queue, log=None):
        self._inbox = Queue()            # producers submit() here
        self._node_queues = node_queues  # {node_id: control queue}
        self._send_queue = send_queue    # exit toward the client
        self._log = log or (lambda msg: None)
        self._thread = threading.Thread(
            target=self._run, name="event_handler", daemon=True)

    def start(self):
        self._thread.start()

    def submit(self, event):
        """Producer-facing inbox: enqueue one event message (dict)."""
        self._inbox.put(json.dumps(event))

    def join(self, timeout=5):
        """Wait for the handler thread to exit (it does after a kill)."""
        if self._thread.is_alive():
            self._thread.join(timeout=timeout)

    def _run(self):
        while True:
            wire = self._inbox.get()
            event = json.loads(wire)
            verb = event.get("signal")
            if verb == "cancel":
                source = event.get("source", 0)   # 0 = pipeline boundary
                event["timestamp"] -= CANCEL_EPSILON
                wire = json.dumps(event)
                self._log(f"event: cancel from {source} dispatched: {event}")
                for nid, q in self._node_queues.items():
                    if nid != source:
                        q.put(wire)
                if source != 0:
                    # server-originated: the client must flush its own side
                    self._send_queue.put(wire)
            elif verb == "kill":
                self._log("event: kill dispatched, event handler exiting")
                for q in self._node_queues.values():
                    q.put(wire)
                return
            else:
                # config-declared event verb: plain broadcast (the entry
                # only admits verbs from the pipeline's events list)
                source = event.get("source", 0)
                self._log(f"event: {verb} from {source} dispatched: {event}")
                for nid, q in self._node_queues.items():
                    if nid != source:
                        q.put(wire)
