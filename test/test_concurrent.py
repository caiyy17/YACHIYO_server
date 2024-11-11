import requests
import asyncio
import websockets
import json
import time
import base64

import os

# FastAPI 服务器的地址
server_url = "http://localhost:8000"
websocket_url = "ws://localhost:8000/ws"

# 测试 POST 请求进行客户端注册
def test_post_register(client_id):
    url = f"{server_url}/register/"
    data = {"client_id": client_id}
    headers = {"Content-Type": "application/json"}  # 发送 JSON 数据时，必须指定 Content-Type 头为 application/json
    try:
        response = requests.post(url, json=data, headers=headers)  # 使用 json 参数来发送 JSON 数据
        if response.status_code == 200:
            print(f"POST /register/: {response.json()}")
        else:
            print(f"POST /register/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

# 测试注销客户端
def test_post_unregister(client_id):
    url = f"{server_url}/unregister/"
    data = {"client_id": client_id}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            print(f"POST /unregister/: {response.json()}")
        else:
            print(f"POST /unregister/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

# 测试未注册客户端注销
def test_post_unregister_unregistered(client_id):
    url = f"{server_url}/unregister/"
    data = {"client_id": client_id}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 400:
            print(f"POST /unregister/ unregistered client: {response.json()}")
        else:
            print(f"POST /unregister/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

# 测试获取所有客户端列表
def test_get_clients():
    url = f"{server_url}/clients/"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"GET /clients/: {response.json()}")
        else:
            print(f"GET /clients/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

# 测试获取单个客户端信息
def test_get_client(client_id):
    url = f"{server_url}/clients/{client_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"GET /clients/{client_id}: {response.json()}")
        else:
            print(f"GET /clients/{client_id} failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

# 测试初始化流水线
def test_init_pipeline(client_id, pipeline_config, force=False):
    url = f"{server_url}/init_pipeline/{client_id}"
    config_data = {"config": json.dumps(pipeline_config), "force": force}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=config_data, headers=headers)
        if response.status_code == 200:
            print(f"POST /init_pipeline/: {response.json()}")
        else:
            print(f"POST /init_pipeline/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

# 测试获取客户端日志
def test_get_client_log(client_id):
    url = f"{server_url}/logs/{client_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"GET /logs/{client_id}: {response.json()}")
        else:
            print(f"GET /logs/{client_id} failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")

# 测试 WebSocket 连接
async def test_websocket(client_id, process_func, messages=[], repeat=1, interval=0, timeout=10):
    start = time.time()
    try:
        async with websockets.connect(f"{websocket_url}/{client_id}", max_size=1024*1024*16) as websocket:
            # 发送消息给服务器
            print(f"start time: {time.time() - start}")
            for i in range(repeat):
                for message in messages:
                    await websocket.send(message)
                    print(f"send time {i}: {time.time() - start}")
                    # 如果太长，只显示前 100 个字符
                    if len(message) > 100:
                        message = message[:100] + "..."
                    print(f"Sent: {message}")
                    await asyncio.sleep(interval)

            # 接收服务器的响应
            index = 0
            while True:
                response = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                print(f"Receive time {index} : {time.time() - start}")
                index += 1
                try:
                    process_func(response)
                except Exception as e:
                    print(f"Error processing response: {e}")
                # 如果太长，只显示前 100 个字符
                if len(response) > 100:
                    response = response[:100] + "..."
                print(f"Received: {response}")
    except asyncio.TimeoutError:
        print("WebSocket receive timeout.")

    except websockets.exceptions.ConnectionClosedError as e:
        print(f"WebSocket connection closed unexpectedly: {e}")
    except websockets.exceptions.InvalidStatusCode as e:
        print(f"WebSocket server returned an invalid status code: {e}")
    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        print(f"WebSocket connection closed for client {client_id}.")

# 运行测试
if __name__ == "__main__":

    # 如果存在test/tmp文件夹，删除
    if os.path.exists("test/tmp"):
        os.system("rm -rf test/tmp")
    os.mkdir("test/tmp")

    num_clients = 3
    client_ids = []
    for i in range(num_clients):
        client_ids.append(f"test-id-{i}")

    # # 测试 POST 注册接口
    for i in range(num_clients):
        client_id = client_ids[i]
        test_post_register(client_id)
        test_get_clients()
        start = time.time()
        json_file = "configs/demo_config.json"
        with open(json_file, "r") as file:
            pipeline_config = json.load(file)
        test_init_pipeline(client_id, pipeline_config, force=True)
        print(f"init time: {time.time() - start}")

    # # 测试 WebSocket 连接
    def process_func(response):
        response = json.loads(response)
        audio_data = response["audio_data"]
        audio_data = base64.b64decode(audio_data)
        timestamp = time.time()
        # 保留4位小数
        with open(f"test/tmp/output_{timestamp:.4f}.wav", "wb") as file:
            file.write(audio_data)

    messages = []
    audio_data = base64.b64encode(open("test/test_voice.wav", "rb").read()).decode("utf-8")
    messages.append(json.dumps({"audio_file": audio_data}))
    # messages.append(json.dumps({"type": "cancel"}))
    # messages.append(json.dumps({"audio_file": audio_data}))

    async def main():
        tasks = []
        for client_id in client_ids:
            print(f"Start testing WebSocket for client {client_id}.")
            task = asyncio.create_task(
                test_websocket(client_id, process_func, messages, repeat=1, interval=0, timeout=5)
            )
            await asyncio.sleep(5)
            tasks.append(task)
        await asyncio.gather(*tasks)

    asyncio.run(main())