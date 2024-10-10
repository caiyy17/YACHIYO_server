import os
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from typing import Dict
from pydantic import BaseModel

import asyncio
import json
import threading
import queue
from queue import Queue
from processing_functions import get_function_by_name

app = FastAPI()

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

# 客户端的类，用于管理每个连接
class ClientConnection:
    def __init__(self, client_id: str, logger: logging.Logger):
        self.client_id = client_id
        self.logger = logger
        self.initialized = False
        self.connected = False

    async def init_pipeline(self, function_names, force=False):
        if self.initialized:
            self.logger.info(f"Client {self.client_id} already initialized")
            if force:
                self.logger.info(f"Force reinitializing client {self.client_id}")
                await self.dispose()
            else:
                return
        
        self.function_names = function_names
        # 队列
        self.send_queue = Queue()
        self.queues = []
        self.cancel_event = []
        self.kill_event = threading.Event()
        # 线程
        self.threads = []
        self.setup_processing_pipeline()
        self.initialized = True

        self.send_task = None
        self.receive_task = None

    def setup_processing_pipeline(self):
        num_functions = len(self.function_names)
        # 创建函数之间的队列
        for _ in range(num_functions):
            self.queues.append(Queue())
            self.cancel_event.append(threading.Event())
        # 将 send_queue 作为最后一个队列
        self.queues.append(self.send_queue)
        self.cancel_event.append(threading.Event())

        # 为每个函数创建线程
        for i, func_name in enumerate(self.function_names):
            func = get_function_by_name(func_name)
            t = threading.Thread(
                target=func,
                args=(
                    self.send_queue,
                    self.queues[i],
                    self.queues[i + 1],
                    self.cancel_event[i],
                    self.cancel_event[i + 1],
                    self.kill_event
                )
            )
            t.start()
            self.threads.append(t)

    async def start_pipeline(self, websocket: WebSocket):
        if self.connected:
            await self.close()
        self.websocket = websocket
        self.connected = True
        # 启动线程监听 send_queue，并将数据发送到 Unity
        self.send_task = asyncio.create_task(self.send_data_to_unity(), name="send_data_to_unity")
        # 启动线程接收 Unity 的数据
        self.receive_task = asyncio.create_task(self.receive_data_from_unity(), name="receive_data_from_unity")

    async def send_data_to_unity(self):
        try:
            while self.connected:
                if self.kill_event.is_set():
                    break
                if self.cancel_event[-1].is_set():
                    while not self.send_queue.empty():
                        self.send_queue.get()
                    self.cancel_event[-1].clear()
                    continue
                try:
                    data = self.send_queue.get(timeout=0.01)
                    # 将数据发送到 Unity，确保数据序列化为字符串
                    await self.websocket.send_text(json.dumps(data))
                    self.logger.info(f"Sent message to {self.client_id}: {data}")
                except queue.Empty:
                    await asyncio.sleep(0.1)
                    pass
                except asyncio.TimeoutError:
                    pass
                except WebSocketDisconnect:
                    self.logger.info(f"Send: Client {self.client_id} disconnected")
                    break
                except Exception as e:
                    self.logger.error(f"Error in receive_data_from_unity: {e}")
        except Exception as e:
            self.logger.error(f"Error in send_data_to_unity: {e}")

    async def receive_data_from_unity(self):
        try:
            while self.connected:
                if self.kill_event.is_set():
                    break
                try:
                    data = await self.websocket.receive_text()
                    self.logger.info(f"Received message from {self.client_id}: {data}")
                    data = json.loads(data)
                    if data.get("type") == "cancel":
                        self.cancel_event[0].set()
                        continue
                    # 将数据放入第一个输入队列
                    self.queues[0].put(data)
                except WebSocketDisconnect:
                    self.connected = False
                    self.logger.info(f"Client {self.client_id} disconnected")
                    self.cancel_event[0].set()
                    break
                except Exception as e:
                    self.logger.error(f"Error in receive_data_from_unity: {e}")
        except Exception as e:
            self.logger.error(f"Error in receive_data_from_unity: {e}")

    async def dispose(self):
        self.logger.info(f"Disposing client {self.client_id}")
        if not self.initialized:
            self.logger.info(f"No dispose: Client {self.client_id} not initialized")
            return
        if self.connected:
            await self.close()
        self.kill_event.set()
        await self.wait_for_threads()
        self.logger.info(f"Client {self.client_id} disposed")
        self.initialized = False
        

    async def wait_for_threads(self):
        while True:
            if not any(t.is_alive() for t in self.threads):
                break
            await asyncio.sleep(0.5)
    
    async def close(self):
        self.logger.info(f"Closing connection for client {self.client_id}")
        if self.connected:
            await self.websocket.close()
            self.connected = False
        else:
            self.logger.info(f"No close: Client {self.client_id} not connected")

        if self.send_task:
            self.send_task.cancel()
            self.send_task = None
        if self.receive_task:
            self.receive_task.cancel()
            self.receive_task = None

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

@app.post("/register/")
async def register_client(data: ClientData):
    if manager.is_registered(data.client_id):
        global_logger.warning(f"Client {data.client_id} already registered")
        return {"status": "registered", "client_id": data.client_id}
        # raise HTTPException(status_code=400, detail="Client already registered")
    # 注册客户端
    manager.create_client(data.client_id)
    manager.register_client(data.client_id)
    global_logger.info(f"Client {data.client_id} registered")
    return {"status": "registered", "client_id": data.client_id}

@app.post("/unregister/")
async def unregister_client(data: ClientData):
    if not manager.is_registered(data.client_id):
        global_logger.warning(f"Client {data.client_id} not registered")
        raise HTTPException(status_code=400, detail="Client not registered")
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
        raise HTTPException(status_code=404, detail="Client not found")
    return {"client_id": client_id}

@app.get("/logs/{client_id}")
async def get_client_log(client_id: str):
    log_filename = f"logs/client_{client_id}.log"
    if not os.path.exists(log_filename):
        raise HTTPException(status_code=404, detail="Log file not found")
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
    function_names = config['function_names']
    await client.init_pipeline()
    return {"status": "initialized", "client_id": client_id}

# WebSocket 处理：为每个连接创建一个独立的实例
@app.websocket("/ws/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    # 首先检查客户端是否已经注册
    if not manager.is_registered(client_id):
        global_logger.warning(f"Client {client_id} not registered")
        await websocket.close()
        raise HTTPException(status_code=400, detail="Client not registered")
    
    # 接受 WebSocket 连接
    await websocket.accept()
    global_logger.info(f"Client {client_id} connected")

    # 连接客户端
    client = manager.clients[client_id]
    function_names = ["call_llm_queue", "llm_text_process_queue", "call_tts_queue", "prepare_response_queue"]
    await client.init_pipeline(function_names)
    await client.start_pipeline(websocket)
    
    try:
        # 处理该客户端的 WebSocket 连接
        while 1:
            if client.receive_task and not client.receive_task.done():
                await asyncio.sleep(1)
            else:
                break
    finally:
        # 客户端断开时移除实例
        global_logger.info(f"Client {client_id} disconnected")
        await manager.remove_client(client_id)


