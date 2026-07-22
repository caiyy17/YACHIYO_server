import uuid

from ..base.BaseProcessingStep import BaseProcessingStep
from ..llm_utils.SimpleHistory import SimpleHistory


class BaseLLMCaller:
    """Pure generation adapter: assembled messages in, chunk stream out.
    History assembly and persistence are the harness manager's job (the
    step orchestrates both); the caller never touches history."""

    def __init__(self, id, config, logger, warmup_messages=None):
        self.client_id = id
        self.config = config
        self.logger = logger
        self.warmup_messages = warmup_messages
        self.custom_init()

    def custom_init(self):
        self.logger.info("LLM caller initialized")

    def cancel(self, cancel_message):
        pass

    def generate(self, messages, allow_tools=True):
        """One generation round over the assembled messages."""
        try:
            yield from self.generate_result(messages,
                                            allow_tools=allow_tools)
        except Exception as e:
            self.logger.error(f"generate error: {e}")

    def generate_result(self, history, allow_tools=True):
        response = {}
        response["text"] = history[-1]["content"]
        response["raw_text"] = history[-1]["content"]
        yield response


class LLMStep(BaseProcessingStep):
    REQUIRED_INPUTS = ["prompt"]

    EMIT_SIGNALS = ["SoS", "EoS"]  # stream envelope: SoS opens, EoS closes
    # playback_complete (control plane): the client's playback report.
    # Configs that don't feed it back wire the source to null.
    REQUIRED_CATCH_EVENTS = ["playback_complete"]

    @classmethod
    def module_outputs(cls, config):
        # per-sentence fields: the cutter's text/raw_text, the generation
        # identity (response_id minted per generation, item_id per
        # sentence — OpenAI-Realtime-style opaque ids the client
        # references in playback reports) plus every config-defined
        # extra_info command channel (action, expression, ...)
        return ["text", "raw_text", "response_id", "item_id"] \
            + list(config.get("extra_info") or {})

    @staticmethod
    def mint_id(kind):
        """Opaque wire id (resp_3f2a... / item_9c41...): minted once per
        generation (resp) or per sentence (item), carried by every chunk
        of that scope and referenced back by the client, never parsed."""
        return f"{kind}_{uuid.uuid4().hex[:12]}"

    def custom_init(self):
        # harness manager: context assembly + the turn's history lifecycle
        self.harness = SimpleHistory(self.client_id, self.config)
        self.llm_caller = BaseLLMCaller(self.client_id, self.config, self.logger)
        # Last finished turn, repairable until the next generation starts:
        # {"response_id": ..., "items": [(item_id, raw_text), ...]}
        self._last_turn = None

    def custom_cancel(self, cancel_message):
        """The COMPLETE cancel semantics (span-style): invalidate caller
        state, conclude the interrupted turn in place — coarse commit of
        the emitted prefix + marker, open the repair window — and close
        the turn (current_timestamp None makes the hook exactly-once and
        lets a report queued right behind this cancel refine within the
        same drain). The polling loop's only job is the fast exit."""
        self.llm_caller.cancel(cancel_message)
        if self.harness.turn_recorded():
            self.harness.commit(interrupted=True)
            self._last_turn = self.harness.turn_identity()
            self.logger.info(
                f"turn interrupted: committed "
                f"{len(self._last_turn['items'])} emitted items")
        self.current_timestamp = None

    def custom_event(self, event):
        """playback_complete {response_id, item_id}: item_id is the item
        that was playing when the client was interrupted — it was heard
        only partially, so history keeps the items STRICTLY BEFORE it and
        the reported item itself is discarded whole. A fully played turn
        sends no truncating report. Only the last turn is repairable — an
        unknown/expired response_id (a new generation already started, or
        a stale report) is ignored."""
        if event.get("signal") != "playback_complete":
            return
        turn = self._last_turn
        rid = event.get("response_id")
        if not turn or rid != turn["response_id"]:
            self.logger.info(
                f"playback report for unknown/expired response "
                f"'{rid}'; ignored")
            return
        item_ids = [i for i, _ in turn["items"]]
        iid = event.get("item_id")
        if iid not in item_ids:
            self.logger.info(
                f"playback report item '{iid}' not in response "
                f"'{rid}'; ignored")
            return
        kept = item_ids.index(iid)
        # refine rebuilds the committed turn from the harness's buffer:
        # the played prefix (plus tool bookkeeping before the cut) with
        # the interruption marker — a coarse cancel-time cut is replaced
        # by the exact played one
        if not self.harness.refine(kept):
            self.logger.info("playback report: no committed turn; ignored")
            return
        self._last_turn = None  # repaired once; later reports are stale
        self.logger.info(
            f"history cut to {kept}/{len(item_ids)} items for "
            f"response '{rid}'")

    def process(self, data, pass_data={}):
        prompt = data.get("prompt", "")
        # Generation identity: response_id spans the whole turn (rides on
        # SoS, every sentence and EoS), item_id is minted per sentence.
        response_id = self.mint_id("resp")
        # the previous turn's repair window closes here
        self._last_turn = None
        self.harness.begin_turn(prompt, response_id)
        # Stream envelope: pass_vars data travels once on the SoS, wrapped
        # under the fixed "pass_data" key (shape built here; emit_signal
        # ships flat); stream messages and EoS carry only the timestamp.
        sos = self.envelope(self.stamp({}, pass_data), pass_data, wrap=True)
        sos["response_id"] = response_id
        self.emit_signal("SoS", sos)
        for response in self.llm_caller.generate(self.harness.assemble()):
            if self.check_cancel():
                # the cancel hook already concluded the turn; fast exit —
                # no EoS: the envelope only closes on natural completion
                self.logger.info("cancel inside loop")
                return
            if response is None:
                continue
            item_id = self.mint_id("item")
            self.harness.record({"item_id": item_id,
                                 "raw_text": response.get("raw_text", "")})
            current_data = {}
            self.add_output(current_data, "response_id", response_id)
            self.add_output(current_data, "item_id", item_id)
            for key, value in response.items():
                self.add_output(current_data, key, value)
            self.output_to_queue(current_data, pass_data,
                                 is_add_pass_data=False)
        # natural completion: close the envelope, open the repair window,
        # write the turn's single history entry
        eos = self.stamp({}, pass_data)
        eos["response_id"] = response_id
        self.emit_signal("EoS", eos)
        self._last_turn = self.harness.turn_identity()
        self.harness.commit()
        return
