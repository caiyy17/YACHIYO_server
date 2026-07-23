import json
import math
import time
from urllib.parse import urlsplit, urlunsplit

from ..base.ChunkGenerationStep import (
    ChunkGenerationCancelled,
    ChunkGenerationSession,
)
from ..motion_base.MotionChunkStep import CHUNK_DURATION_MS, MotionChunkStep
from .MotionGenerationStep import _b64_decode_f32, _humanoid_to_frames
from .smplh_to_humanoid import smplh_to_humanoid
from utils.settings import get_setting


class MotionWebSocketError(RuntimeError):
    pass


def websocket_url(base):
    """Convert an HTTP service base (or preserve a WS URL) and add /ws."""
    parts = urlsplit(base)
    scheme = {"http": "ws", "https": "wss"}.get(parts.scheme, parts.scheme)
    if scheme not in ("ws", "wss") or not parts.netloc:
        raise ValueError(f"invalid motion websocket base: {base!r}")
    path = parts.path.rstrip("/")
    if not path.endswith("/ws"):
        path += "/ws"
    return urlunsplit((scheme, parts.netloc, path, parts.query, ""))


def _connect(uri, **kwargs):
    # Lazy import keeps unrelated base modules importable in minimal installs;
    # the project requirement is pinned to a version with the sync API.
    from websockets.sync.client import connect
    return connect(uri, **kwargs)


class MotionWebSocketSession(ChunkGenerationSession):
    """Span-scoped Flood session with one request and one block per input.

    A span's motion hint is captured at SoS/reset and remains unchanged until
    EoS. The first input sends ``start`` and every later input sends exactly one
    ``continue``; each request produces one fixed-size Motion chunk. ``finish``
    closes the WebSocket so the remote exclusive slot is released at EoS.
    """

    def __init__(
        self,
        url,
        frames_per_chunk,
        framerate,
        *,
        logger,
        motion_hint_key="motion_hint",
        humanoid_output=True,
        seed=42,
        cfg_scale=5.0,
        history_length=30,
        smoothing_alpha=0.5,
        connect_timeout=5.0,
        receive_timeout=10.0,
        poll_interval=0.1,
        cancel_check=None,
        connector=None,
    ):
        self.url = websocket_url(url)
        self.frames_per_chunk = frames_per_chunk
        self.framerate = framerate
        self.logger = logger
        self.motion_hint_key = motion_hint_key
        self.humanoid_output = humanoid_output
        self.start_options = {
            "seed": seed,
            "cfg_scale": cfg_scale,
            "history_length": history_length,
            "smoothing_alpha": smoothing_alpha,
            "stream_size": frames_per_chunk,
        }
        self.connect_timeout = connect_timeout
        self.receive_timeout = receive_timeout
        self.poll_interval = poll_interval
        self.cancel_check = cancel_check
        self.connector = connector or _connect
        self.connection = None
        self._fresh = True
        self._motion_hint = ""
        self._format = None
        self._first_frame = True
        self._prev_trans = None
        self._ref_y = None
        self.connect()

    def connect(self):
        if self.connection is not None:
            return
        connection = self.connector(
            self.url,
            open_timeout=self.connect_timeout,
            close_timeout=1,
            max_size=None,
        )
        self.connection = connection
        # A busy Flood service accepts the handshake, immediately sends an
        # error, then closes. Detect that during the init probe / span open.
        try:
            raw = connection.recv(timeout=0.05)
        except TimeoutError:
            return
        except Exception:
            self.connection = None
            try:
                connection.close()
            finally:
                raise
        try:
            event = self._parse_event(raw)
            if event.get("type") == "error":
                error = event.get("error")
                message = error.get("message") if isinstance(error, dict) \
                    else error
                raise MotionWebSocketError(str(message or error or event))
            raise MotionWebSocketError(
                f"unexpected event while opening session: {event}"
            )
        finally:
            self.connection = None
            connection.close()

    def reset(self, start_context=None):
        motion_hint = self._motion_hint_from(start_context or {})
        if not motion_hint:
            raise ValueError(
                f"stream start needs a non-empty motion hint in "
                f"'{self.motion_hint_key}'"
            )
        if self.connection is None:
            self.connect()
        self._reset_local()
        self._motion_hint = motion_hint

    def generate_chunk(self, inputs, chunk_index):
        if not self._motion_hint:
            raise RuntimeError("motion session has not been reset for a span")
        try:
            primary, secondary, first = self._request_chunk()
            count = len(primary)
            if first and count == self.frames_per_chunk + 1:
                # Flood's start delta contains one bootstrap frame followed by
                # the requested timeline. Dropping that first frame makes the
                # first output 1..N and the following continue N+1..2N.
                if secondary is not None and self.humanoid_output:
                    # Preserve the anchor for the first emitted frame's root
                    # delta and session-relative pelvis height.
                    self._prev_trans = [
                        float(value) for value in secondary[0]
                    ]
                    self._ref_y = float(secondary[0][1])
                primary = primary[1:]
                if secondary is not None:
                    secondary = secondary[1:]
            elif count != self.frames_per_chunk:
                raise MotionWebSocketError(
                    f"motion server returned {count} frames for "
                    f"{'start' if first else 'continue'}; expected "
                    f"{self.frames_per_chunk}"
                    + (f" or {self.frames_per_chunk + 1}" if first else "")
                )
            return {"motion": self._package(primary, secondary)}
        except ChunkGenerationCancelled:
            raise
        except Exception:
            self.abort()
            raise

    def finish(self):
        # Flood has no finish message. Closing the transport is the session
        # boundary and releases the server's exclusive slot at TTS EoS.
        self._disconnect()

    def abort(self):
        self._disconnect()

    def _disconnect(self):
        connection, self.connection = self.connection, None
        self._reset_local()
        if connection is not None:
            try:
                connection.close()
            except Exception as error:
                self.logger.error(
                    f"motion websocket close failed: "
                    f"{type(error).__name__}: {error}"
                )

    def close(self):
        self.abort()

    def _clear_generation(self):
        self._format = None
        self._first_frame = True
        self._prev_trans = None
        self._ref_y = None

    def _reset_local(self):
        self._clear_generation()
        self._fresh = True
        self._motion_hint = ""

    def _motion_hint_from(self, values):
        value = values.get(self.motion_hint_key)
        if value is None:
            return ""
        if not isinstance(value, str):
            raise TypeError(
                f"motion hint '{self.motion_hint_key}' must be a string"
            )
        return value.strip()

    def _request_chunk(self):
        if self.connection is None:
            self.connect()
        first = self._fresh
        if first:
            message = {"type": "start", "text": self._motion_hint}
            message.update({
                key: value for key, value in self.start_options.items()
                if value is not None
            })
            self._send(message)
            started = self._receive("session.started")
            self._accept_started(started)
            delta = self._receive("motion.delta")
            self._fresh = False
        else:
            self._send({"type": "continue", "text": self._motion_hint})
            delta = self._receive("motion.delta")
        primary, secondary = self._decode_delta(delta)
        return primary, secondary, first

    def _send(self, message):
        try:
            self.connection.send(json.dumps(message))
        except Exception:
            self.abort()
            raise

    def _receive(self, expected_type):
        deadline = time.monotonic() + self.receive_timeout
        while True:
            if self.cancel_check is not None and self.cancel_check():
                self.abort()
                raise ChunkGenerationCancelled
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"motion websocket timed out waiting for {expected_type}"
                )
            try:
                raw = self.connection.recv(
                    timeout=min(self.poll_interval, remaining)
                )
            except TimeoutError:
                continue
            except Exception:
                self.abort()
                raise
            event = self._parse_event(raw)
            if event.get("type") == "error":
                error = event.get("error")
                message = error.get("message") if isinstance(error, dict) \
                    else error
                raise MotionWebSocketError(str(message or error or event))
            if event.get("type") != expected_type:
                raise MotionWebSocketError(
                    f"expected {expected_type}, got {event.get('type')!r}"
                )
            return event

    @staticmethod
    def _parse_event(raw):
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if not isinstance(raw, str):
            raise MotionWebSocketError(
                f"websocket event must be text, got {type(raw).__name__}"
            )
        try:
            event = json.loads(raw)
        except json.JSONDecodeError as error:
            raise MotionWebSocketError("invalid JSON from motion server") \
                from error
        if not isinstance(event, dict):
            raise MotionWebSocketError("motion websocket event must be an object")
        return event

    def _accept_started(self, event):
        fmt = event.get("format")
        if fmt not in ("smplh", "joints22"):
            raise MotionWebSocketError(f"unsupported motion format: {fmt!r}")
        if fmt == "joints22" and self.humanoid_output:
            raise MotionWebSocketError(
                "joints22 cannot be converted to humanoid output"
            )
        try:
            actual_fps = float(event["framerate"])
        except (KeyError, TypeError, ValueError) as error:
            raise MotionWebSocketError("invalid session framerate") from error
        if abs(actual_fps - self.framerate) > 1e-6:
            raise MotionWebSocketError(
                f"server framerate {actual_fps} does not match configured "
                f"framerate {self.framerate}"
            )
        self._format = fmt

    def _decode_delta(self, event):
        if self._format is None:
            raise MotionWebSocketError("motion delta arrived before session.started")
        count = event.get("num_frames")
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise MotionWebSocketError(f"invalid delta num_frames: {count!r}")

        try:
            if self._format == "smplh":
                if event.get("poses_shape") != [count, 156] \
                        or event.get("trans_shape") != [count, 3]:
                    raise MotionWebSocketError("invalid smplh delta shape")
                primary = _b64_decode_f32(
                    event["poses"], event["poses_shape"]
                )
                secondary = _b64_decode_f32(
                    event["trans"], event["trans_shape"]
                )
            else:
                if event.get("joints_shape") != [count, 22, 3]:
                    raise MotionWebSocketError("invalid joints22 delta shape")
                primary = _b64_decode_f32(
                    event["joints"], event["joints_shape"]
                )
                secondary = None
        except MotionWebSocketError:
            raise
        except Exception as error:
            raise MotionWebSocketError("invalid motion delta payload") from error

        return primary, secondary

    def _package(self, primary, secondary):
        if self._format == "smplh" and self.humanoid_output:
            result = smplh_to_humanoid(
                primary,
                secondary,
                len(primary),
                framerate=self.framerate,
                prev_trans=self._prev_trans,
                ref_y=self._ref_y,
            )
            if self._ref_y is None:
                self._ref_y = float(secondary[0][1])
            self._prev_trans = [float(value) for value in secondary[-1]]
            frames = _humanoid_to_frames(result)
            fmt = "humanoid"
        elif self._format == "smplh":
            frames = [
                {
                    "poses": primary[index].tolist(),
                    "trans": secondary[index].tolist(),
                }
                for index in range(len(primary))
            ]
            fmt = "smplh"
        else:
            frames = [
                {"joints": primary[index].tolist()}
                for index in range(len(primary))
            ]
            fmt = "joints22"

        if self._first_frame:
            frames[0] = {
                "header": {
                    "framerate": self.framerate,
                    "format": fmt,
                },
                **frames[0],
            }
            self._first_frame = False
        return frames


class MotionChunkGenerationStep(MotionChunkStep):
    """Fixed chunks over a Flood WebSocket owned only for each TTS span."""

    @classmethod
    def validate_config(cls, config):
        errors = super().validate_config(config)
        for key, default in (
                ("connect_timeout", 5.0),
                ("receive_timeout", 10.0),
                ("poll_interval", 0.1)):
            value = config.get(key, default)
            if isinstance(value, bool) \
                    or not isinstance(value, (int, float)) or value <= 0:
                errors.append(f"{key} must be a number > 0, got {value!r}")
        motion_hint_key = config.get("motion_hint_key", "motion_hint")
        if not isinstance(motion_hint_key, str) or not motion_hint_key:
            errors.append(
                f"motion_hint_key must be a non-empty string, got "
                f"{motion_hint_key!r}"
            )
        init_test_text = config.get("init_test_text", "test")
        if not isinstance(init_test_text, str) or not init_test_text.strip():
            errors.append(
                f"init_test_text must be a non-empty string, got "
                f"{init_test_text!r}"
            )
        init_test_duration_ms = config.get("init_test_duration_ms", 1000)
        if isinstance(init_test_duration_ms, bool) \
                or not isinstance(init_test_duration_ms, (int, float)) \
                or init_test_duration_ms <= 0:
            errors.append(
                f"init_test_duration_ms must be a number > 0, got "
                f"{init_test_duration_ms!r}"
            )
        return errors

    def generation_init(self):
        super().generation_init()
        self.motion_session = None
        model_name = self.get_config("model", "flood")
        with open("configs/settings/motion.json", encoding="utf-8") as file:
            model_config = json.load(file)[model_name]
        extra = model_config.get("extra", {})
        base = self.get_config("ws_url")
        if not base:
            api_key = model_config.get("api_base") or "motion_api"
            base = get_setting("motion_generation", api_key)

        self.motion_session = MotionWebSocketSession(
            base,
            self.frames_per_chunk,
            self.framerate,
            logger=self.logger,
            motion_hint_key=self.get_config(
                "motion_hint_key", "motion_hint"
            ),
            humanoid_output=self.get_config("humanoid_output", True),
            seed=self.get_config("seed", extra.get("seed", 42)),
            cfg_scale=self.get_config(
                "cfg_scale", extra.get("cfg_scale", 5.0)
            ),
            history_length=self.get_config("history_length", 30),
            smoothing_alpha=self.get_config("smoothing_alpha", 0.5),
            connect_timeout=self.get_config("connect_timeout", 5.0),
            receive_timeout=self.get_config("receive_timeout", 10.0),
            poll_interval=self.get_config("poll_interval", 0.1),
            cancel_check=self.check_cancel,
        )
        # Init is fail-fast at the generation level, not merely handshake
        # level: generate at least the configured test duration, validate
        # every returned fixed chunk, then immediately release the remote
        # exclusive slot. reset() reconnects at the actual tts_SoS.
        init_test_text = self.get_config("init_test_text", "test").strip()
        init_test_duration_ms = self.get_config(
            "init_test_duration_ms", 1000
        )
        chunk_duration_ms = self.get_config(
            "chunk_duration_ms", CHUNK_DURATION_MS
        )
        init_chunks = max(
            1, math.ceil(init_test_duration_ms / chunk_duration_ms)
        )
        try:
            self.motion_session.reset({
                self.get_config("motion_hint_key", "motion_hint"):
                    init_test_text,
            })
            for index in range(init_chunks):
                self.motion_session.generate_chunk({}, index)
        except Exception:
            self.motion_session.abort()
            raise
        else:
            self.motion_session.finish()
        self.logger.info(
            f"motion init probe generated {init_chunks} chunks / "
            f"{init_chunks * self.frames_per_chunk} frames / "
            f"{init_chunks * chunk_duration_ms}ms, then released session"
        )

    def open_generation_session(self, start_context):
        try:
            self.motion_session.reset(start_context)
        except Exception:
            self.motion_session.abort()
            raise
        return self.motion_session

    def generation_dispose(self):
        if self.motion_session is not None:
            self.motion_session.close()
