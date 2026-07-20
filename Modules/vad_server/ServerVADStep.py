import base64
import json

import requests

from ..vad_base.VADStep import VADStep


class ServerVADCaller:
    """Session client for the streaming-VAD service: create a session,
    append raw-PCM chunks, get the detector events back (custom HTTP
    protocol; field names are the domain's generic terms). One streaming
    session per pipeline node."""

    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        config_name = config.get("model", "energy_vad")
        with open("configs/settings/vad.json", "r") as f:
            self.model_config = json.load(f)[config_name]
        from utils.settings import get_setting
        self.base = get_setting("vad", self.model_config["api_base"])
        self.session_id = self._create_session()

    def _create_session(self):
        body = {
            "model": self.model_config.get("model_name", "energy"),
            "threshold": self.model_config.get("threshold", 0.5),
            "silence_duration_ms":
                self.model_config.get("silence_duration_ms", 300),
            "sample_rate": self.config.get("sample_rate", 48000),
        }
        r = requests.post(f"{self.base}/v1/audio/vad/sessions",
                          json=body, timeout=5)
        r.raise_for_status()
        sid = r.json()["session_id"]
        self.logger.info(f"vad service session {sid} ({body})")
        return sid

    def feed(self, pcm_b64):
        """One raw-PCM16 chunk in, the chunk's detector events out."""
        r = requests.post(
            f"{self.base}/v1/audio/vad/sessions/{self.session_id}/append",
            json={"audio": pcm_b64}, timeout=5)
        r.raise_for_status()
        return r.json().get("events", [])

    def close(self):
        try:
            requests.delete(
                f"{self.base}/v1/audio/vad/sessions/{self.session_id}",
                timeout=2)
        except Exception:
            pass

    def reset(self):
        """Fresh session: drops the detector's in_speech/debounce state."""
        self.close()
        self.session_id = self._create_session()


class ServerVADStep(VADStep):
    """Model-driven VAD: the segment machinery is the base class's, the
    signal source is a detector service instead of client signals.

    Every ingested chunk is also fed to the service — the exact
    rate-normalized bytes the ring ingested (single decode); its events
    drive the
    turn — speech_started opens a barge-in: the node submits a cancel
    event (stamp = the activation chunk's stamp, i.e. the new turn's own;
    source = this node's id) through the pipeline's event handler, which
    broadcasts it to every OTHER node and out to the client; the excluded
    emitter handles its own state instead (it aligns its cancel watermark
    itself, and the new turn's pre-roll is safe in the segment's own
    buffer). Then the segment starts exactly like a caught recording_start
    (vad_start emitted downstream, mark with the configured lead), and
    speech_stopped finalizes it after the detector tail end_offset_ms
    (default 0: the stop verdict already contains silence_duration_ms of
    confirmed silence).

    Config `auto_detect` (default true) is the master switch for the
    detector: false runs the node purely signal-driven — no service
    session, no auto events, no barge-in cancels; turns come only from the
    caught recording signals below.

    Margins are per source: signed `start_offset_ms` / `end_offset_ms` shape
    DETECTOR turns (the end defaults to 0 — the stop verdict already
    contains silence_duration_ms of confirmed silence), while
    `manual_start_offset_ms` /
    `manual_end_offset_ms` shape SIGNAL turns (manual = the standalone
    client signals; the end tail covers e.g. the DC hold lag).

    Client recording signals take MANUAL control: a caught recording_start
    opens the turn through the base machinery and SUSPENDS detection (no
    chunks are fed to the service, no auto events fire) until that turn
    completes (recording_end plus the tail), after which detection resumes
    with a fresh detector session; a cancel voiding the manual turn also
    resumes detection. A recording_end with no manual turn active is
    ignored (auto turns are the detector's to close). Configs that want a
    purely detector-driven node wire both catches to null."""

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        v = config.get("auto_detect", True)
        if not isinstance(v, bool):
            errors.append(f"auto_detect must be a bool, got {v!r}")
        ring_s = config.get("ring_seconds", 60)
        ms = config.get("start_offset_ms", 0)
        if isinstance(ms, bool) or not isinstance(ms, (int, float)):
            errors.append(
                f"start_offset_ms must be a signed number, got {ms!r}")
        elif ms < 0 \
                and isinstance(ring_s, (int, float)) \
                and not isinstance(ring_s, bool) \
                and ms <= -ring_s * 1000:
            errors.append(f"start_offset_ms ({ms}) must be > "
                          f"-ring_seconds*1000")
        me = config.get("end_offset_ms", 0)
        if isinstance(me, bool) or not isinstance(me, (int, float)):
            errors.append(
                f"end_offset_ms must be a signed number, got {me!r}")
        return errors

    def span_init(self):
        super().span_init()
        self.auto_detect = self.get_config("auto_detect", True)
        self._manual = False   # client-driven turn active: detection off
        # per-source margins: the base parsed the MANUAL pair (its signals
        # ARE the manual source); the detector pair is this module's own
        self._manual_start = self.start_offset
        self._manual_end = self.end_offset
        self._auto_start = int(self.get_config("start_offset_ms", 0)
                               * self.sample_rate / 1000)
        self._auto_end = int(self.get_config("end_offset_ms", 0)
                             * self.sample_rate / 1000)
        if not self.auto_detect:
            # passive mode: signal-driven only, the service is never needed
            self._detector = None
            self._events = None
            self.logger.info("auto detection disabled - passive mode")
            return
        if self._auto_start == 0:
            self.logger.warning(
                "auto_detect with start_offset_ms=0: detection latency "
                "(~100-150ms) will clip utterance heads; configure a "
                "negative lookback")
        self._detector = ServerVADCaller(self.config, self.logger)
        # the pipeline's event handler, injected by the server at build
        # time; without it the node cannot emit the barge-in cancel
        self._events = self.get_config("__events")
        if self._events is None:
            self.init_error = "no event handler injected (__events)"

    def span_process(self, data, pass_data={}):
        if not self.auto_detect:
            super().span_process(data, pass_data)   # pure signal-driven
            return
        signal = data.get("signal", "")
        if signal == "recording_start":
            self.logger.info("client recording_start - detection suspended")
            self._manual = True
            self.start_offset = self._manual_start
            self.end_offset = self._manual_end
            super().span_process(data, pass_data)
            return
        if signal == "recording_end":
            if not self._manual:
                self.logger.info(
                    "client recording_end without a manual turn - ignored")
                return
            super().span_process(data, pass_data)
            if self._mark is None:   # no tail to wait for: turn done now
                self._resume_detection()
            return

        msg_ts = data.get("timestamp")
        pcm = super().span_process(data, pass_data)
        if self._manual:
            if self._mark is None:   # tail filled, manual turn finalized
                self._resume_detection()
            return                   # suspended: nothing goes to the service
        if pcm is None:
            return
        try:
            # the exact bytes the ring ingested (already rate-normalized)
            events = self._detector.feed(
                base64.b64encode(pcm).decode("ascii"))
        except Exception as e:
            self.logger.error(f"vad service call failed: {e}")
            return
        for event in events:
            if event.get("type") == "speech_started":
                self._on_activation(msg_ts)
            elif event.get("type") == "speech_stopped":
                self.logger.info("detector: speech_stopped")
                # the stop verdict already sat through
                # silence_duration_ms of confirmed silence; the detector
                # tail (end_offset_ms, default 0) is extra on top
                if self._mark is not None:
                    self._on_end()

    def _on_activation(self, chunk_ts):
        """Barge-in: cancel everything older than this turn everywhere but
        here, then open the segment (order: cancel first, then start)."""
        self.logger.info("detector: speech_started - barge-in")
        # the semantic stamp is the turn's own; the event handler applies
        # the numerical epsilon uniformly on dispatch
        self._events.submit({
            "signal": "cancel",
            "timestamp": chunk_ts,
            "source": self.index,
        })
        # the dispatch excludes this node, so align our own watermark to the
        # cancel we just minted — the emitter handles its own state (the
        # segment is safe: it owns its audio, and strict < spares the
        # turn's own same-stamp messages)
        self.cancel_timestamp = max(self.cancel_timestamp, chunk_ts)
        self.start_offset = self._auto_start
        self.end_offset = self._auto_end
        self._on_start({"timestamp": chunk_ts})

    def on_span_cancel(self, cancel_message):
        """A cancel voiding the active segment must also reset the
        detector: it still believes it is mid-speech, so without a fresh
        session the rest of the ongoing utterance would never re-trigger
        speech_started and would be lost until the next pause."""
        super().on_span_cancel(cancel_message)
        self._manual = False   # a voided manual turn hands control back
        if self._detector is None:
            return
        try:
            self._detector.reset()
            self.logger.info("cancel - detector session reset")
        except Exception as e:
            self.logger.error(f"detector reset failed: {e}")

    def _resume_detection(self):
        """Manual turn over: hand control back to the detector with a
        fresh session (its pre-suspension state is stale)."""
        self._manual = False
        try:
            self._detector.reset()
        except Exception as e:
            self.logger.error(f"detector reset failed: {e}")
        self.logger.info("manual turn done - detection resumed")

    def custom_dispose(self):
        if self._detector is not None:
            self._detector.close()
