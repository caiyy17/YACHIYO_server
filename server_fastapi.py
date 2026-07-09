import os
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from typing import Dict
from pydantic import BaseModel

# Tolerance for float timestamp comparison (covers JSON serialization precision loss)
TIMESTAMP_EPSILON = 1e-3

from starlette.responses import PlainTextResponse
from starlette.middleware.cors import CORSMiddleware

import asyncio
import json
import threading
import queue
from queue import Queue
from Modules import get_function_class_by_name

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

        self.send_queue = Queue()
        self.send_cancel_queue = Queue()
        self.send_task = None
        self.receive_task = None
        self.kill_event = threading.Event()

        self.queues = [self.send_queue]
        self.cancel_queues = [self.send_cancel_queue]
        self.threads = []
        self.websocket = None
        self.last_timestamp = 0

        self.logger.info(f"Client {self.client_id} created")

    def log_info(self, message, cut=True):
        if cut and len(message) > MESSAGE_MAX_LENGTH:
            message = message[:MESSAGE_MAX_LENGTH] + "..."
        self.logger.info(f"{message}")

    async def init_pipeline(self, pipeline_config, force=False):
        if self.initialized:
            self.log_info(f"Init: Client {self.client_id} already initialized")
            if force:
                self.log_info(f"Init: Force reinitializing client {self.client_id}")
                await self.dispose()
            else:
                return

        self.pipeline_config = pipeline_config
        self.log_info(
            f"Init: Initializing client {self.client_id} with pipeline: {self.pipeline_config}",
            cut=False,
        )
        self.queues = []
        self.cancel_queues = []
        self.threads = []
        try:
            self.setup_processing_pipeline()
        except PipelineInitError:
            # Kill nodes that already started, wait them out, and restore a
            # clean re-initializable state (dispose() can't be used here: it
            # guards on self.initialized which is still False).
            self.kill_event.set()
            await self.wait_for_threads()
            self.kill_event.clear()
            self.queues = [self.send_queue]
            self.cancel_queues = [self.send_cancel_queue]
            self.threads = []
            self.initialized = False
            raise
        self.initialized = True
        self.log_info(f"Init: Client {self.client_id} initialized")

    def setup_processing_pipeline(self):
        pipeline = self.pipeline_config["pipeline"]
        num_functions = len(pipeline)
        # Create queues between functions
        # Each node can set max_queue_size in its config to limit its input queue capacity.
        # 0 (default) means unbounded. When full, upstream put() blocks until space is freed.
        for i in range(num_functions):
            max_queue_size = pipeline[i].get("config", {}).get("max_queue_size", 0)
            self.queues.append(Queue(maxsize=max_queue_size))
            self.cancel_queues.append(Queue())
        # Recreate send_queue with optional max_queue_size from pipeline config
        send_max = self.pipeline_config.get("send_queue_max_size", 0)
        self.send_queue = Queue(maxsize=send_max)
        self.queues.append(self.send_queue)
        self.cancel_queues.append(self.send_cancel_queue)

        # Create a thread for each function. Node initialization is
        # sequential; a failed init (dependent service down) FAILS FAST:
        # nodes built so far are killed and the error surfaces to the caller
        # instead of leaving a silently half-broken pipeline.
        for i, node in enumerate(pipeline):
            func_name = node["function"]  # Get current node's function name
            func_class = get_function_class_by_name(func_name)  # Get corresponding class
            config = node.get("config", {})  # Get config
            self.log_info(
                f"Init: Creating thread for function {func_name} with config: {config}"
            )
            func_instance = func_class(
                node["node_id"],
                self.client_id,
                self.logger,
                self.send_queue,
                self.queues[i],
                self.queues[i + 1],
                self.cancel_queues[i],
                self.kill_event,
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
            self.log_info(f"Start: No start: Client {self.client_id} not initialized")
            return
        await self.close()
        self.websocket = websocket
        self.connected = True
        while not self.send_queue.empty():
            self.send_queue.get()
        while not self.send_cancel_queue.empty():
            self.send_cancel_queue.get()
        # Start task to listen on send_queue and send data to client
        self.send_task = asyncio.create_task(self.send_data(), name="send_data")
        # Start task to receive data from client
        self.receive_task = asyncio.create_task(
            self.receive_data(), name="receive_data"
        )
        self.log_info(f"Start: Client {self.client_id} pipeline started")

    async def send_data(self):
        cancel_timestamp = 0
        while self.connected:
            try:
                if self.kill_event.is_set():
                    break

                # Check for cancel messages
                if not self.send_cancel_queue.empty():
                    while not self.send_cancel_queue.empty():
                        cancel = self.send_cancel_queue.get()
                    cancel = json.loads(cancel)
                    self.log_info(f"Sent: received cancel signal: {cancel}")
                    cancel_timestamp = cancel["timestamp"]

                # Get data from send_queue
                try:
                    data = self.send_queue.get(timeout=0)
                except queue.Empty:
                    await asyncio.sleep(TIME_INTERVAL)
                    continue
                data_dict = json.loads(data)
                if data_dict.get("timestamp", float("inf")) < cancel_timestamp:
                    self.log_info(f"Sent: Skipping data: {data}")
                    continue

                # Strip internal pipeline fields before sending to client
                data_dict.pop("destination", None)
                data = json.dumps(data_dict, ensure_ascii=False)

                # Send data to client
                await self.websocket.send_text(data)
                # Calculate data size
                data_size = len(data.encode("utf-8"))
                self.log_info(f"Sent: message to {self.client_id}: {data_size} bytes")
                self.log_info(f"Sent: message to {self.client_id}: {data}")

            except asyncio.TimeoutError:
                pass
            except WebSocketDisconnect:
                pass
            except Exception as e:
                self.logger.error(f"Sent: {e}")

        while not self.send_cancel_queue.empty():
            cancel = self.send_cancel_queue.get()
            cancel = json.loads(cancel)
            self.log_info(f"Sent: received cancel signal: {cancel}")
            cancel_timestamp = cancel["timestamp"]
        while not self.send_queue.empty():
            data = self.send_queue.get()
            self.log_info(f"Sent: Skipping data: {data}")
        self.log_info(f"Sent: Client {self.client_id} disconnected")

    async def receive_data(self):
        while self.connected:
            if self.kill_event.is_set():
                break

            # Socket-level receive: any failure here means the connection is
            # gone (abrupt close raises a RuntimeError, not WebSocketDisconnect)
            # — stop the loop instead of spinning on the same error forever,
            # which would starve the event loop and hang dispose/unregister.
            try:
                raw = await self.websocket.receive_text()
            except WebSocketDisconnect:
                self.log_info(f"Received: Client {self.client_id} disconnected")
                self.connected = False
                for q in self.cancel_queues:
                    q.put(json.dumps({"signal": "cancel", "timestamp": self.last_timestamp + TIMESTAMP_EPSILON}))
                break
            except Exception as e:
                self.logger.error(f"Received: socket receive failed, stopping: {e}")
                self.connected = False
                for q in self.cancel_queues:
                    q.put(json.dumps({"signal": "cancel", "timestamp": self.last_timestamp + TIMESTAMP_EPSILON}))
                break

            # Message-level parsing/handling: a bad message is skipped, not fatal
            try:
                data = json.loads(raw)
                if "timestamp" not in data:
                    self.logger.error(f"Received: missing timestamp in message: {data}")
                    continue

                # Cancel bypasses monotonic check (its timestamp refers to what to cancel)
                if data.get("signal", "") == "cancel":
                    self.log_info(f"Received: cancel signal: {data}")
                    for q in self.cancel_queues:
                        q.put(json.dumps(data))
                    continue

                if data["timestamp"] < self.last_timestamp:
                    self.logger.error(
                        f"Received: timestamp not monotonic: "
                        f"{data['timestamp']} < {self.last_timestamp}"
                    )
                    continue
                self.last_timestamp = data["timestamp"]
                self.log_info(f"Received: message from {self.client_id}, {data}")

                # Put data into the first input queue
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
        if self.connected:
            await self.close()
        self.kill_event.set()
        await self.wait_for_threads()
        if self.send_task:
            self.send_task.cancel()
            self.send_task = None
        if self.receive_task:
            self.receive_task.cancel()
            self.receive_task = None
        self.kill_event.clear()
        self.queues = [self.send_queue]
        self.cancel_queues = [self.send_cancel_queue]
        self.threads = []
        self.initialized = False
        self.log_info(f"Dispose: Client {self.client_id} disposed")

    async def wait_for_threads(self, timeout=35):
        """Wait for threads to exit. Timeout should exceed the longest HTTP timeout
        in any module (e.g. MotionGeneration 30s) to allow graceful exit."""
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            if not any(t.is_alive() for t in self.threads):
                return
            await asyncio.sleep(TIME_INTERVAL)
        alive = [t.name for t in self.threads if t.is_alive()]
        if alive:
            self.log_info(f"Dispose: threads still alive after {timeout}s: {alive}")

    async def close(self):
        if self.connected:
            self.log_info(f"Close: Closing connection for client {self.client_id}")
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
            await self.clients[client_id].dispose()
            del self.clients[client_id]
            self.registered_clients.remove(client_id)

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
    force: bool = False


@app.post("/heartbeat/")
async def heartbeat(data: ClientData):
    return {"status": "alive", "client_id": data.client_id}


@app.post("/register/")
async def register_client(data: ClientData):
    if manager.is_registered(data.client_id):
        global_logger.warning(f"Client {data.client_id} already registered")
        return {"status": "already registered", "client_id": data.client_id}
    # Register client
    manager.register_client(data.client_id)
    manager.create_client(data.client_id)
    global_logger.info(f"Client {data.client_id} registered")
    return {"status": "registered", "client_id": data.client_id}


@app.post("/unregister/")
async def unregister_client(data: ClientData):
    if not manager.is_registered(data.client_id):
        global_logger.warning(f"Client {data.client_id} not registered")
        return {"status": "not registered", "client_id": data.client_id}
    # Remove client
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
        return {"log_content": ""}
    with open(log_filename, "r") as f:
        log_content = f.read()
    return {"log_content": log_content}


@app.post("/init_pipeline/{client_id}")
async def init_pipeline(client_id: str, data: ConfigData):
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

    # Static validation before building: strictly PER-NODE self-consistency
    # (each node's config vs its own module contract — required catches,
    # required inputs, emit/dispatch references). No cross-module flow
    # modeling; broken links between nodes surface at runtime via the
    # four-state signal rules. Reject on any finding, with the details in
    # the response and the client log.
    from utils.pipeline_validator import validate_pipeline
    errors, warnings = validate_pipeline(pipeline_config, get_function_class_by_name)
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


async def reject_websocket_connection(websocket: WebSocket):
    """
    Reject WebSocket connection with HTTP 403 Forbidden response.
    """
    # Build HTTP 403 Forbidden response
    response = PlainTextResponse(
        "Client not registered", status_code=status.HTTP_403_FORBIDDEN
    )

    # Convert response to ASGI format and send
    await response(
        scope=websocket.scope, receive=websocket.receive, send=websocket.send
    )


# WebSocket handler: create an independent instance for each connection
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    # First check if the client is registered
    if not manager.is_registered(client_id):
        global_logger.warning(f"Client {client_id} not registered")
        await reject_websocket_connection(websocket)
        return

    # Accept WebSocket connection
    await websocket.accept()
    global_logger.info(f"Client {client_id} connected")

    # Connect client
    client = manager.clients[client_id]
    await client.start_pipeline(websocket)

    try:
        # Handle the client's WebSocket connection
        while 1:
            if client.connected:
                await asyncio.sleep(TIME_INTERVAL)
            else:
                break
    finally:
        global_logger.info(f"Client {client_id} disconnected, disposing pipeline")
        await client.dispose()
