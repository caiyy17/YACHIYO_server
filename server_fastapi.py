import os
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, status
from typing import Dict
from pydantic import BaseModel

from starlette.responses import PlainTextResponse

import time
import asyncio
import json
import threading
import queue
from queue import Queue
from processing_functions import get_function_class_by_name

app = FastAPI()
TIME_INTERVAL = 0.05
MESSAGE_MAX_LENGTH = 200

if not os.path.exists("tmp"):
    os.makedirs("tmp")

# 设置全局日志处理器
def setup_global_logger():
    logger = logging.getLogger("global_logger")
    logger.setLevel(logging.INFO)

    # 日志目录
    log_directory = "logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # 全局日志文件
    log_filename = os.path.join(log_directory, "global_log.log")
    
    # 创建文件处理器 (file handler) 并设置格式
    file_handler = logging.FileHandler(log_filename, mode='a')  # 'a' 表示追加模式
    console_handler = logging.StreamHandler()  # 控制台处理器

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 将处理器添加到日志记录器中
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("---------- Global logger initialized ----------")
    return logger

# 初始化全局 logger
global_logger = setup_global_logger()

def log_info(self, message):
    if len(message) > MESSAGE_MAX_LENGTH:
        message = message[:MESSAGE_MAX_LENGTH] + "..."
    global_logger.info(message)

# 客户端的类，用于管理每个连接
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
                self.log_info(f"Init: Client {self.client_id} reinitialized")
            else:
                return
        
        self.pipeline_config = pipeline_config
        self.log_info(f"Init: Initializing client {self.client_id} with pipeline: {self.pipeline_config}", cut=False)
        self.queues = []
        self.cancel_queues = []
        self.threads = []
        self.setup_processing_pipeline()
        self.initialized = True
        self.log_info(f"Init: Client {self.client_id} initialized")

    def setup_processing_pipeline(self):
        pipeline = self.pipeline_config["pipeline"]
        num_functions = len(pipeline)
        # 创建函数之间的队列
        for _ in range(num_functions):
            self.queues.append(Queue())
            self.cancel_queues.append(Queue())
        # 将 send_queue 作为最后一个队列
        self.queues.append(self.send_queue)
        self.cancel_queues.append(self.send_cancel_queue)

        # 为每个函数创建线程
        for i, node in enumerate(pipeline):
            func_name = node["function"]  # 获取当前节点的函数名称
            func_class = get_function_class_by_name(func_name)  # 获取对应的类
            config = node.get("config", {})  # 获取配置信息
            self.log_info(f"Init: Creating thread for function {func_name} with config: {config}")
            func_instance = func_class(
                node["node_id"],
                self.client_id,
                self.logger,
                self.send_queue,
                self.queues[i],
                self.queues[i + 1],
                self.cancel_queues[i],
                self.cancel_queues[i + 1],
                self.kill_event,
                config
            )
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
        # 启动线程监听 send_queue，并将数据发送到 Unity
        self.send_task = asyncio.create_task(self.send_data(), name="send_data")
        # 启动线程接收 Unity 的数据
        self.receive_task = asyncio.create_task(self.receive_data(), name="receive_data")
        self.log_info(f"Start: Client {self.client_id} pipeline started")

    async def send_data(self):
        cancel_timestamp = 0
        while self.connected:
            try:
                if self.kill_event.is_set():
                        break
                
                # 检查是否有取消消息
                if not self.send_cancel_queue.empty():
                    while not self.send_cancel_queue.empty():
                        cancel = self.send_cancel_queue.get()
                    cancel = json.loads(cancel)
                    self.log_info(f"Sent: received cancel signal: {cancel}")
                    cancel_timestamp = cancel["timestamp"]
                
                # 从 send_queue 中获取数据
                try:
                    data = self.send_queue.get(timeout=0)
                except queue.Empty:
                    await asyncio.sleep(TIME_INTERVAL)
                    continue
                if cancel_timestamp > 0 and json.loads(data)["timestamp"] < cancel_timestamp:
                    self.log_info(f"Sent: Skipping data: {data}")
                    continue

                # 将数据发送到 Unity，确保数据序列化为字符串
                await self.websocket.send_text(data)
                # 计算data的大小
                data_size = len(data.encode('utf-8'))
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

            try:
                data = await self.websocket.receive_text()
                data = json.loads(data)
                data["timestamp"] = time.time()
                self.log_info(f"Received: message from {self.client_id}, {data}")

                if data.get("type") == "cancel":
                    for q in self.cancel_queues:
                        q.put(json.dumps(data))
                    continue

                # 将数据放入第一个输入队列
                data = json.dumps(data)
                self.queues[0].put(data)

            except WebSocketDisconnect:
                self.connected = False
                self.log_info(f"Received: Client {self.client_id} disconnected")
                timestamp = time.time()
                for q in self.cancel_queues:
                    q.put(json.dumps({"type": "cancel", "timestamp": timestamp}))
                break
            except Exception as e:
                self.logger.error(f"Received: {e}")

    async def dispose(self):
        self.log_info(f"Dispose: Disposing client {self.client_id}")
        if not self.initialized:
            self.log_info(f"Dispose: No dispose: Client {self.client_id} not initialized")
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

    async def wait_for_threads(self):
        while True:
            if not any(t.is_alive() for t in self.threads):
                break
            await asyncio.sleep(TIME_INTERVAL)

    async def close(self):
        if self.connected:
            self.log_info(f"Close: Closing connection for client {self.client_id}")
            await self.websocket.close()
        else:
            self.log_info(f"Close: No close: Client {self.client_id} not connected")

        if self.receive_task:
            self.log_info(f"Close: Waiting for receive_task to finish for client {self.client_id}")
            await self.receive_task
            self.receive_task = None
        if self.send_task:
            self.log_info(f"Close: Waiting for send_task to finish for client {self.client_id}")
            await self.send_task
            self.send_task = None
        
        self.websocket = None
        self.connected = False
        self.log_info(f"Close: Connection reset for client {self.client_id}")

# 管理器类，维护所有客户端连接的实例
class ClientManager:
    def __init__(self):
        self.clients: Dict[str, ClientConnection] = {}
        self.registered_clients = set()  # 存储已注册的 client_id

    # 注册客户端
    def register_client(self, client_id: str):
        self.registered_clients.add(client_id)

    # 检查是否注册
    def is_registered(self, client_id: str) -> bool:
        return client_id in self.registered_clients

    # 创建客户端连接
    def create_client(self, client_id: str):
        # 为每个客户端创建单独的日志文件
        logger = self.setup_logger(client_id)
        client = ClientConnection(client_id, logger)
        self.clients[client_id] = client
        return client

    # 移除客户端连接
    async def remove_client(self, client_id: str):
        if client_id in self.clients:
            await self.clients[client_id].dispose()
            del self.clients[client_id]
            self.registered_clients.remove(client_id)

    def setup_logger(self, client_id: str) -> logging.Logger:
        # 创建日志记录器
        logger = logging.getLogger(client_id)
        logger.setLevel(logging.INFO)

        # 创建日志目录
        log_directory = "logs"
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

        # 为每个客户端创建单独的日志文件
        log_filename = os.path.join(log_directory, f"client_{client_id}.log")
        if os.path.exists(log_filename):
            os.remove(log_filename)
        file_handler = logging.FileHandler(log_filename)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # 为当前 logger 添加 file handler
        if not logger.hasHandlers():
            logger.addHandler(file_handler)

        logger.info(f"---------- Logger initialized for client {client_id} ----------")

        return logger

# 创建全局管理器实例
manager = ClientManager()

# 定义数据模型
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
    # 注册客户端
    manager.register_client(data.client_id)
    manager.create_client(data.client_id)
    global_logger.info(f"Client {data.client_id} registered")
    return {"status": "registered", "client_id": data.client_id}

@app.post("/unregister/")
async def unregister_client(data: ClientData):
    if not manager.is_registered(data.client_id):
        global_logger.warning(f"Client {data.client_id} not registered")
        return {"status": "not registered", "client_id": data.client_id}
    # 移除客户端
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
    return {"status": "connected", "client_id": client_id}

@app.get("/logs/{client_id}")
async def get_client_log(client_id: str):
    log_filename = f"logs/client_{client_id}.log"
    if not os.path.exists(log_filename):
        return {"log_content": ""}
    with open(log_filename, 'r') as f:
        log_content = f.read()
    return {"log_content": log_content}

@app.post("/init_pipeline/{client_id}")
async def init_pipeline(client_id: str, data: ConfigData):
    if client_id not in manager.clients:
        raise HTTPException(status_code=404, detail="Client not found")
    client = manager.clients[client_id]
    config_message = data.config
    config = json.loads(config_message)
    await client.init_pipeline(config, force=data.force)
    return {"status": "initialized", "client_id": client_id}

async def reject_websocket_connection(websocket: WebSocket):
    """
    拒绝 WebSocket 连接，返回 HTTP 403 Forbidden 响应
    """
    # 构建 HTTP 403 Forbidden 响应
    response = PlainTextResponse("Client not registered", status_code=status.HTTP_403_FORBIDDEN)

    # 将响应转换为 ASGI 格式并发送
    await response(scope=websocket.scope, receive=websocket.receive, send=websocket.send)

# WebSocket 处理：为每个连接创建一个独立的实例
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    # 首先检查客户端是否已经注册
    if not manager.is_registered(client_id):
        global_logger.warning(f"Client {client_id} not registered")
        await reject_websocket_connection(websocket)
        return
    
    # 接受 WebSocket 连接
    await websocket.accept()
    global_logger.info(f"Client {client_id} connected")

    # 连接客户端
    client = manager.clients[client_id]
    json_file = "default_config.json"
    with open(json_file, 'r') as f:
        pipeline_config = json.load(f)
    await client.init_pipeline(pipeline_config)
    await client.start_pipeline(websocket)
    
    try:
        # 处理该客户端的 WebSocket 连接
        while 1:
            if client.connected:
                await asyncio.sleep(TIME_INTERVAL)
            else:
                break
    finally:
        global_logger.info(f"Client {client_id} disconnected")
