import requests
import asyncio
import websockets
import json

# FastAPI 服务器的地址
server_url = "http://localhost:5003"
websocket_url = "ws://localhost:5003/ws"

# 测试 POST 请求
def test_post_register(client_id):
    url = f"{server_url}/register/"
    data = {"client_id": client_id}
    headers = {"Content-Type": "application/json"}  # 发送 JSON 数据时，必须指定 Content-Type 头为 application/json
    response = requests.post(url, json=data, headers=headers)  # 使用 json 参数来发送 JSON 数据

    if response.status_code == 200:
        print(f"POST /register/: {response.json()}")
    else:
        print(f"POST /register/ failed: {response.status_code}, {response.text}")

# 测试 WebSocket 连接
async def test_websocket(client_id):
    async with websockets.connect(f"{websocket_url}/{client_id}") as websocket:
        try:
            # 发送消息给服务器
            message = json.dumps({"message": "Hello, WebSocket!"})
            await websocket.send(message)
            print(f"Sent: {message}")

            # 接收服务器的响应
            response = await websocket.recv()
            print(f"Received: {response}")
        except Exception as e:
            print(f"WebSocket error: {e}")
        finally:
            # 显式关闭 WebSocket 连接
            await websocket.close()
            print(f"WebSocket connection closed.")

# 运行测试
if __name__ == "__main__":
    client_id = "test-client"

    # 测试 POST 接口
    test_post_register(client_id)

    # 测试 WebSocket 连接
    asyncio.get_event_loop().run_until_complete(test_websocket(client_id))
