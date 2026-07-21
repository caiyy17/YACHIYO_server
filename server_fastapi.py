import os
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from typing import Dict
from pydantic import BaseModel

from starlette.responses import PlainTextResponse
from starlette.middleware.cors import CORSMiddleware

import asyncio
import json
import math
import threading
import queue
from collections import deque
from queue import Queue
from Modules import get_function_class_by_name
from utils.event_handler import EventHandler

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
TIME_INTERVAL = 0.05
MESSAGE_MAX_LENGTH = 200


# Set up global logger
def setup_global_logger():
    logger = logging.getLogger("global_logger")
    logger.setLevel(logging.INFO)

    # Log directory
    log_directory = "logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Global log file
    log_filename = os.path.join(log_directory, "global_log.log")

    # Create file handler and set format
    file_handler = logging.FileHandler(log_filename, mode="a")  # 'a' for append mode
    console_handler = logging.StreamHandler()  # Console handler

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("---------- Global logger initialized ----------")
    return logger


# Initialize global logger
global_logger = setup_global_logger()


class PipelineInitError(Exception):
    """A node failed to initialize (dependent service down / bad settings).
    Raised by setup_processing_pipeline after killing already-started nodes;
    init_pipeline surfaces it as HTTP 503 with the node detail."""


# Client class for managing each connection
class ClientConnection:
    def __init__(self, client_id: str, logger: logging.Logger):
        self.client_id = client_id
        self.logger = logger
        self.initialized = False
        self.connected = False
        # This client's current init-built runtime may be attached once.  A
        # different client ID may independently use the same pipeline config.
        self.connection_used = False
        self.pipeline_config = None

        self.send_queue = Queue()
        self.send_task = None
        self.receive_task = None

        self.queues = [self.send_queue]
        # Control plane: one event handler per pipeline (built in setup,
        # torn down with it); all control-verb traffic goes through its
        # inbox via submit(). The config's top-level `events` list names
        # the verbs the entry routes there (cancel is built-in).
        self.events = None
        self.event_verbs = set()
        self.threads = []
        self.websocket = None
        self.last_timestamp = 0  # entry monotonicity floor (non-cancel)
        # Per-pipeline log threshold (config top-level __debug_level, set at
        # init): messages print when their level >= this; 0 = everything,
        # 1 (default) = hide per-message content traffic
        self.debug_level = 1

        self.logger.info(f"Client {self.client_id} created")

    def log_info(self, message, cut=True, level=1):
        # message prints when its level >= the pipeline's __debug_level
        # threshold (per-message content traffic passes level=0)
        if level < self.debug_level:
            return
        if cut and len(message) > MESSAGE_MAX_LENGTH:
            message = message[:MESSAGE_MAX_LENGTH] + "..."
        self.logger.info(f"{message}")

    async def init_pipeline(self, pipeline_config, force=False):
        if self.initialized and not self.connection_used and not force:
            self.log_info(f"Init: Client {self.client_id} already initialized")
            return

        if self.initialized:
            self.log_info(f"Init: Reinitializing client {self.client_id}")
            await self.dispose()

        self.pipeline_config = pipeline_config
        self.debug_level = int(pipeline_config.get("__debug_level", 1) or 0)
        self.event_verbs = set(pipeline_config.get("events") or [])
        self.last_timestamp = 0
        self.log_info(
            f"Init: Initializing client {self.client_id} with pipeline: {self.pipeline_config}",
            cut=False,
        )
        self.queues = []
        self.events = None
        self.threads = []
        try:
            self.setup_processing_pipeline()
        except Exception:
            # Kill nodes that already started, wait them out, and restore a
            # clean re-initializable state (dispose() can't be used here: it
            # guards on self.initialized which is still False). Catching
            # broadly: ANY failure mid-build would otherwise orphan the
            # already-started node threads.
            if self.events:
                self.events.submit({"signal": "cancel", "timestamp": float("inf"),
                                    "source": 0})
                self.events.submit({"signal": "kill"})
            await self.wait_for_threads()
            if self.events:
                self.events.join()
            self.queues = [self.send_queue]
            self.events = None
            self.threads = []
            self.initialized = False
            raise
        self.initialized = True
        self.connection_used = False
        self.log_info(f"Init: Client {self.client_id} initialized")

    def setup_processing_pipeline(self):
        pipeline = self.pipeline_config["pipeline"]
        num_functions = len(pipeline)
        # Create queues between functions
        # Each node can set max_queue_size in its config to limit its input queue capacity.
        # 0 (default) means unbounded. When full, upstream put() blocks until space is freed.
        cancel_queues = {}
        for i in range(num_functions):
            max_queue_size = pipeline[i].get("config", {}).get("max_queue_size", 0)
            self.queues.append(Queue(maxsize=max_queue_size))
            cancel_queues[pipeline[i]["node_id"]] = Queue()
        # Recreate send_queue with optional max_queue_size from pipeline config
        send_max = self.pipeline_config.get("send_queue_max_size", 0)
        self.send_queue = Queue(maxsize=send_max)
        self.queues.append(self.send_queue)

        # Control plane: the pipeline owns queue construction (control
        # queues alongside data queues, keyed by node id for source-aware
        # dispatch); the event handler receives them plus the send queue
        # (server-originated cancels go out to the client) and is the only
        # writer into them. Started before the nodes so a mid-build failure
        # can already tear down via its inbox.
        self.events = EventHandler(cancel_queues, self.send_queue,
                                   log=self.log_info)
        self.events.start()

        # Create a thread for each function. Node initialization is
        # sequential; a failed init (dependent service down) FAILS FAST:
        # nodes built so far are killed and the error surfaces to the caller
        # instead of leaving a silently half-broken pipeline.
        for i, node in enumerate(pipeline):
            func_name = node["function"]  # Get current node's function name
            func_class = get_function_class_by_name(func_name)  # Get corresponding class
            # Copy so the stored pipeline_config stays as loaded; nodes see
            # the pipeline-wide debug level as a config key
            config = dict(node.get("config", {}))
            config["__debug_level"] = self.debug_level
            self.log_info(
                f"Init: Creating thread for function {func_name} with config: {config}"
            )
            # runtime-only handle (injected after the log so configs log
            # clean): a node with event capability submits control events
            # (e.g. a model-driven VAD's barge-in cancel) through this
            config["__events"] = self.events
            func_instance = func_class(
                node["node_id"],
                self.client_id,
                self.logger,
                self.send_queue,
                self.queues[i],
                self.queues[i + 1],
                cancel_queues[node["node_id"]],
                config,
            )
            if getattr(func_instance, "init_error", None):
                detail = (
                    f"node[{i}] {func_name}(id={node['node_id']}): "
                    f"{func_instance.init_error}"
                )
                self.logger.error(f"Init: pipeline init failed — {detail}")
                raise PipelineInitError(detail)
            t = threading.Thread(target=func_instance.run, name=f"{i}_{func_name}")
            t.start()
            self.threads.append(t)

    async def start_pipeline(self, websocket: WebSocket):
        if not self.initialized:
            raise RuntimeError(f"Client {self.client_id} is not initialized")
        if self.connection_used:
            raise RuntimeError(
                f"Client {self.client_id} must reinitialize before reconnecting"
            )
        self.websocket = websocket
        self.connected = True
        self.connection_used = True
        while not self.send_queue.empty():
            self.send_queue.get()
        # Start task to listen on send_queue and send data to client
        self.send_task = asyncio.create_task(self.send_data(), name="send_data")
        # Start task to receive data from client
        self.receive_task = asyncio.create_task(
            self.receive_data(), name="receive_data"
        )
        self.log_info(f"Start: Client {self.client_id} pipeline started")

    async def send_data(self):
        """Pure relay: send_queue -> client. No gating here — dropping is
        each module's own responsibility (module-specific policies)."""
        while self.connected:
            try:
                try:
                    data = self.send_queue.get(timeout=0)
                except queue.Empty:
                    await asyncio.sleep(TIME_INTERVAL)
                    continue
                data_dict = json.loads(data)

                # Strip internal pipeline fields before sending to client
                data_dict.pop("destination", None)
                data = json.dumps(data_dict, ensure_ascii=False)

                # Send data to client
                await self.websocket.send_text(data)
                self.log_info(f"Sent: message to {self.client_id}: "
                              f"{len(data.encode('utf-8'))} bytes: {data}",
                              level=0)

            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                pass
            except Exception as e:
                self.logger.error(f"Sent: {e}")

        while not self.send_queue.empty():
            data = self.send_queue.get()
            self.log_info(f"Sent: Skipping data: {data}", level=0)
        self.log_info(f"Sent: Client {self.client_id} disconnected")

    async def receive_data(self):
        """Client -> pipeline relay. The entry validates message IDENTITY —
        a numeric timestamp must be present (cancel included), and non-cancel
        timestamps must be monotonic (equal allowed, smaller rejected) — so
        modules downstream can trust the stamp without re-checking. Content
        is never gated here: dropping is each module's own responsibility
        (module-specific policies). The only routing decision is the control
        plane (cancel -> every node's cancel queue). Teardown on disconnect
        is the endpoint's job (dispose broadcasts cancel(inf) + kill)."""
        while self.connected:
            # Socket-level receive: any failure here means the connection is
            # gone (abrupt close raises a RuntimeError, not WebSocketDisconnect)
            # — stop the loop instead of spinning on the same error forever,
            # which would starve the event loop and hang dispose/unregister.
            try:
                raw = await self.websocket.receive_text()
            except WebSocketDisconnect:
                self.log_info(f"Received: Client {self.client_id} disconnected")
                self.connected = False
                break
            except Exception as e:
                if self.connected:
                    self.logger.error(
                        f"Received: socket receive failed, stopping: {e}")
                else:   # our own close() unblocked the receive — routine
                    self.log_info(f"Received: connection closed locally: {e}")
                self.connected = False
                break

            # Message-level parsing/handling: a bad message is skipped, not fatal
            try:
                data = json.loads(raw)
                ts = data.get("timestamp")
                if (
                    isinstance(ts, bool)
                    or not isinstance(ts, (int, float))
                    or (isinstance(ts, float) and math.isnan(ts))
                ):
                    self.logger.error(
                        f"Received: missing/invalid timestamp, dropped: {data}")
                    continue

                # Control-plane verbs — cancel (built-in) plus the config's
                # top-level events list — are exempt from monotonicity
                # (their timestamps refer to the past) and route to the
                # event handler
                verb = data.get("signal", "")
                if verb == "cancel" or verb in self.event_verbs:
                    # boundary events are source 0 — FORCED here, so a
                    # client cannot impersonate a node (exclusion and
                    # client-echo routing key off the source)
                    data["source"] = 0
                    self.events.submit(data)
                    continue

                if ts < self.last_timestamp:
                    self.logger.error(
                        f"Received: timestamp not monotonic: "
                        f"{ts} < {self.last_timestamp}, dropped: {data}")
                    continue
                self.last_timestamp = ts

                self.log_info(
                    f"Received: message from {self.client_id}, {data}",
                    level=0)
                self.queues[0].put(json.dumps(data))
            except Exception as e:
                self.logger.error(f"Received (message): {e}")

    async def dispose(self):
        self.log_info(f"Dispose: Disposing client {self.client_id}")
        if not self.initialized:
            self.log_info(
                f"Dispose: No dispose: Client {self.client_id} not initialized"
            )
            return
        await self.close()
        self.events.submit({"signal": "cancel", "timestamp": float("inf"),
                        "source": 0})
        self.events.submit({"signal": "kill"})
        await self.wait_for_threads()
        self.events.join()
        if self.send_task:
            self.send_task.cancel()
            self.send_task = None
        if self.receive_task:
            self.receive_task.cancel()
            self.receive_task = None
        self.queues = [self.send_queue]
        self.events = None
        self.event_verbs = set()
        self.threads = []
        self.initialized = False
        self.log_info(f"Dispose: Client {self.client_id} disposed")

    async def wait_for_threads(self):
        """Wait until every node from this pipeline has actually exited."""
        while any(t.is_alive() for t in self.threads):
            await asyncio.sleep(TIME_INTERVAL)

    async def close(self):
        if self.connected:
            self.log_info(f"Close: Closing connection for client {self.client_id}")
            # declare the shutdown BEFORE closing the socket: the receive
            # loop distinguishes a peer failure (ERROR) from our own close
            # (routine) by this flag
            self.connected = False
            try:
                await self.websocket.close()
            except Exception as e:
                # Peer may have closed already (e.g. rapid reconnect race);
                # a failed close of a stale socket must not kill the new one.
                self.log_info(f"Close: websocket already closed: {e}")
        else:
            self.log_info(f"Close: No close: Client {self.client_id} not connected")

        if self.receive_task:
            self.log_info(
                f"Close: Waiting for receive_task to finish for client {self.client_id}"
            )
            await self.receive_task
            self.receive_task = None
        if self.send_task:
            self.log_info(
                f"Close: Waiting for send_task to finish for client {self.client_id}"
            )
            await self.send_task
            self.send_task = None

        self.websocket = None
        self.connected = False
        self.log_info(f"Close: Connection reset for client {self.client_id}")


# Manager class that maintains all client connection instances
class ClientManager:
    def __init__(self):
        self.clients: Dict[str, ClientConnection] = {}
        self.registered_clients = set()  # Store registered client_ids
        # Pipeline initialization is intentionally global and sequential.
        self.pipeline_init_lock = asyncio.Lock()
        # Locks outlive ClientConnection objects so waiters for the same ID
        # can never split across two different locks during unregister.
        self.lifecycle_locks: Dict[str, asyncio.Lock] = {}

    def lifecycle_lock(self, client_id: str) -> asyncio.Lock:
        lock = self.lifecycle_locks.get(client_id)
        if lock is None:
            lock = asyncio.Lock()
            self.lifecycle_locks[client_id] = lock
        return lock

    # Register client
    def register_client(self, client_id: str):
        self.registered_clients.add(client_id)

    # Check if registered
    def is_registered(self, client_id: str) -> bool:
        return client_id in self.registered_clients

    # Create client connection
    def create_client(self, client_id: str):
        # Create a separate log file for each client
        logger = self.setup_logger(client_id)
        client = ClientConnection(client_id, logger)
        self.clients[client_id] = client
        return client

    # Remove client connection
    async def remove_client(self, client_id: str):
        if client_id in self.clients:
            client = self.clients[client_id]
            await client.dispose()
            if self.clients.get(client_id) is client:
                del self.clients[client_id]
            self.registered_clients.discard(client_id)

    def setup_logger(self, client_id: str) -> logging.Logger:
        # Create logger
        logger = logging.getLogger(client_id)
        logger.setLevel(logging.INFO)

        # Create log directory
        log_directory = "logs"
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        # Create a separate log file for each client
        log_filename = os.path.join(log_directory, f"client_{client_id}.log")
        if os.path.exists(log_filename):
            os.remove(log_filename)
        file_handler = logging.FileHandler(log_filename)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        # Replace old handlers (logger is cached by name, old handlers may point to deleted files)
        for h in logger.handlers[:]:
            logger.removeHandler(h)
            h.close()
        logger.addHandler(file_handler)

        logger.info(f"---------- Logger initialized for client {client_id} ----------")

        return logger


# Create global manager instance
manager = ClientManager()


# Define data models
class ClientData(BaseModel):
    client_id: str


class ConfigData(BaseModel):
    config: str
    # A runtime that has already served a WebSocket is always rebuilt.
    # force also rebuilds an initialized runtime that has not been used yet.
    force: bool = False


@app.post("/heartbeat/")
async def heartbeat(data: ClientData):
    return {"status": "alive", "client_id": data.client_id}


@app.post("/register/")
async def register_client(data: ClientData):
    async with manager.lifecycle_lock(data.client_id):
        if manager.is_registered(data.client_id):
            global_logger.warning(f"Client {data.client_id} already registered")
            return {"status": "already registered", "client_id": data.client_id}
        # Registration only grants this ID permission to initialize.
        manager.create_client(data.client_id)
        manager.register_client(data.client_id)
        global_logger.info(f"Client {data.client_id} registered")
        return {"status": "registered", "client_id": data.client_id}


@app.post("/unregister/")
async def unregister_client(data: ClientData):
    async with manager.lifecycle_lock(data.client_id):
        if not manager.is_registered(data.client_id):
            global_logger.warning(f"Client {data.client_id} not registered")
            return {"status": "not registered", "client_id": data.client_id}
        await manager.remove_client(data.client_id)
        global_logger.info(f"Client {data.client_id} unregistered")
        return {"status": "unregistered", "client_id": data.client_id}


@app.get("/clients/")
async def get_clients():
    return {"clients": list(manager.clients.keys())}


@app.get("/clients/{client_id}")
async def get_client(client_id: str):
    if client_id not in manager.clients:
        return {"status": "not connected", "client_id": client_id}
    response = {"status": "connected", "client_id": client_id}
    # present once the pipeline has been initialized
    config = getattr(manager.clients[client_id], "pipeline_config", None)
    if config is not None:
        response["pipeline_config"] = config
    return response


@app.get("/logs/{client_id}")
async def get_client_log(client_id: str):
    log_filename = f"logs/client_{client_id}.log"
    if not os.path.exists(log_filename):
        raise HTTPException(
            status_code=404,
            detail={"error": "client log not found", "client_id": client_id},
        )
    if not os.path.isfile(log_filename):
        raise HTTPException(
            status_code=400,
            detail={"error": "client log path is not a file", "client_id": client_id},
        )
    try:
        with open(log_filename, "r", encoding="utf-8") as f:
            log_content = "".join(deque(f, maxlen=200))
    except (OSError, UnicodeError) as e:
        global_logger.error(f"Client {client_id} log read failed: {e}")
        raise HTTPException(
            status_code=500,
            detail={
                "error": "client log read failed",
                "client_id": client_id,
                "detail": str(e),
            },
        ) from e
    return {"log_content": log_content}


@app.post("/init_pipeline/{client_id}")
async def init_pipeline(client_id: str, data: ConfigData):
    async with manager.pipeline_init_lock, manager.lifecycle_lock(client_id):
        if client_id not in manager.clients:
            raise HTTPException(status_code=404, detail="Client not found")

        client = manager.clients[client_id]
        config_name = data.config
        json_file = f"configs/{config_name}.json"
        # Unknown config = client-side error: reject explicitly (no fallback —
        # silently building a different pipeline than requested misleads debugging)
        if not os.path.exists(json_file):
            global_logger.warning(
                f"Client {client_id} config file not found: {config_name}"
            )
            raise HTTPException(
                status_code=404,
                detail={"error": "config not found", "config": config_name},
            )
        global_logger.info(f"Client {client_id} config file: {config_name}")
        with open(json_file, "r") as file:
            pipeline_config = json.load(file)

        # Static validation before building: per-node self-consistency (each
        # node's config vs its own module contract — required catches,
        # required inputs, emit/dispatch references; no cross-module flow
        # modeling, broken links surface at runtime via the four-state
        # signal rules) plus the pipeline-level control-plane check (top-level
        # events list == union of catch_events sources). Reject on any
        # finding, with the details in the response and the client log.
        from utils.pipeline_validator import validate_pipeline
        errors, warnings = validate_pipeline(
            pipeline_config, get_function_class_by_name
        )
        findings = errors + warnings
        if findings:
            for f in findings:
                client.logger.error(f"pipeline validation [{config_name}]: {f}")
            raise HTTPException(
                status_code=400,
                detail={"error": "pipeline validation failed",
                        "config": config_name, "findings": findings},
            )

        try:
            await client.init_pipeline(pipeline_config, force=data.force)
        except PipelineInitError as e:
            # Configuration is valid (validator passed) but a dependent service
            # failed at node init — 503 so the client can retry once it is up.
            raise HTTPException(
                status_code=503,
                detail={"error": "pipeline node init failed",
                        "config": config_name, "detail": str(e)},
            )
        return {"status": "initialized", "client_id": client_id}


async def reject_websocket_connection(
    websocket: WebSocket,
    message="Client not registered",
    status_code=status.HTTP_403_FORBIDDEN,
):
    """Reject a WebSocket during the HTTP handshake."""
    response = PlainTextResponse(message, status_code=status_code)

    # Convert response to ASGI format and send
    await response(
        scope=websocket.scope, receive=websocket.receive, send=websocket.send
    )


# WebSocket handler: create an independent instance for each connection
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    lifecycle_lock = manager.lifecycle_lock(client_id)
    async with lifecycle_lock:
        if not manager.is_registered(client_id):
            global_logger.warning(f"Client {client_id} not registered")
            await reject_websocket_connection(websocket)
            return

        client = manager.clients[client_id]
        if not client.initialized:
            global_logger.warning(
                f"Client {client_id} must initialize before WebSocket connect"
            )
            await reject_websocket_connection(
                websocket,
                "Pipeline must be initialized before WebSocket connect",
                status.HTTP_409_CONFLICT,
            )
            return
        if client.connection_used:
            global_logger.warning(
                f"Client {client_id} must reinitialize before WebSocket reconnect"
            )
            await reject_websocket_connection(
                websocket,
                "Pipeline must be reinitialized before WebSocket reconnect",
                status.HTTP_409_CONFLICT,
            )
            return

        await websocket.accept()
        await client.start_pipeline(websocket)
        global_logger.info(f"Client {client_id} connected")

    try:
        # Handle the client's WebSocket connection
        while client.websocket is websocket and client.connected:
            await asyncio.sleep(TIME_INTERVAL)
    finally:
        async with lifecycle_lock:
            current_client = manager.clients.get(client_id)
            if (
                current_client is client
                and client.websocket is websocket
            ):
                global_logger.info(
                    f"Client {client_id} disconnected, disposing pipeline"
                )
                await client.dispose()
