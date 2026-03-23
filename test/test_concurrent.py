import requests
import asyncio
import websockets
import json
import time
import base64

import os

# FastAPI server address
server_url = "http://localhost:8910"
websocket_url = "ws://localhost:8910/ws"


# Test POST request for client registration
def test_post_register(client_id):
    url = f"{server_url}/register/"
    data = {"client_id": client_id}
    headers = {
        "Content-Type": "application/json"
    }  # Content-Type header must be set to application/json when sending JSON data
    try:
        response = requests.post(
            url, json=data, headers=headers
        )  # Use json parameter to send JSON data
        if response.status_code == 200:
            print(f"POST /register/: {response.json()}")
        else:
            print(f"POST /register/ failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test client unregistration
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


# Test unregistering an unregistered client
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


# Test getting all clients list
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


# Test getting single client info
def test_get_client(client_id):
    url = f"{server_url}/clients/{client_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"GET /clients/{client_id}: {response.json()}")
        else:
            print(
                f"GET /clients/{client_id} failed: {response.status_code}, {response.text}"
            )
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test pipeline initialization
def test_init_pipeline(client_id, pipeline_config, force=False):
    url = f"{server_url}/init_pipeline/{client_id}"
    config_data = {"config": pipeline_config, "force": force}
    headers = {"Content-Type": "application/json"}
    try:
        response = requests.post(url, json=config_data, headers=headers)
        if response.status_code == 200:
            print(f"POST /init_pipeline/: {response.json()}")
        else:
            print(
                f"POST /init_pipeline/ failed: {response.status_code}, {response.text}"
            )
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test getting client logs
def test_get_client_log(client_id):
    url = f"{server_url}/logs/{client_id}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"GET /logs/{client_id}: {response.json()}")
        else:
            print(
                f"GET /logs/{client_id} failed: {response.status_code}, {response.text}"
            )
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")


# Test WebSocket connection
async def test_websocket(
    client_id, process_func, messages=[], repeat=1, interval=0, timeout=10
):
    start = time.time()
    try:
        async with websockets.connect(
            f"{websocket_url}/{client_id}", max_size=1024 * 1024 * 16
        ) as websocket:
            # Send messages to server
            print(f"start time: {time.time() - start}")
            for i in range(repeat):
                for message in messages:
                    await websocket.send(message)
                    print(f"send time {i}: {time.time() - start}")
                    # Truncate to first 100 characters if too long
                    if len(message) > 100:
                        message = message[:100] + "..."
                    print(f"Sent: {message}")
                    await asyncio.sleep(interval)

            # Receive server responses
            index = 0
            while True:
                response = await asyncio.wait_for(websocket.recv(), timeout=timeout)
                print(f"Receive time {index} : {time.time() - start}")
                index += 1
                try:
                    process_func(response)
                except Exception as e:
                    print(f"Error processing response: {e}")
                # Truncate to first 100 characters if too long
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


# Run tests
if __name__ == "__main__":
    # Remove test/tmp directory if it exists
    if os.path.exists("test/tmp"):
        os.system("rm -rf test/tmp")
    os.mkdir("test/tmp")

    pipeline_config = "unity_chan"
    messages = []

    # Add messages
    audio_data = base64.b64encode(open("test/test_voice.wav", "rb").read()).decode(
        "utf-8"
    )
    messages.append(json.dumps({"text": "你好，世界！", "audio_file": audio_data, "timestamp": time.time()}))

    # messages.append(json.dumps({"signal": "cancel", "timestamp": time.time()}))
    # messages.append(json.dumps({"audio_file": audio_data, "timestamp": time.time()}))

    client_id = "test-id-0"
    force = True

    # Test POST register endpoint
    test_post_register(client_id)

    # Test getting clients list
    test_get_clients()

    # Test getting single client
    test_get_client(client_id)

    # Test pipeline initialization
    start = time.time()
    test_init_pipeline(client_id, pipeline_config, force=force)
    print(f"init time: {time.time() - start}")

    # Test getting client logs
    # test_get_client_log(client_id)

    # Test WebSocket connection
    def process_func(response):
        response = json.loads(response)
        timestamp = time.time()
        # Keep 4 decimal places

        # Save audio data to file
        try:
            audio_data = response["audio_data"]
            audio_data = base64.b64decode(audio_data)
            with open(f"test/tmp/output_{timestamp:.4f}.wav", "wb") as file:
                file.write(audio_data)
        except Exception as e:
            print(f"Error saving audio file: {e}")

        # Save response to file
        try:
            with open(f"test/tmp/response_{timestamp:.4f}.json", "w") as file:
                json.dump(response, file, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"Error saving response file: {e}")

        

    asyncio.run(
        test_websocket(
            client_id, process_func, messages, repeat=1, interval=0, timeout=10
        )
    )

    # # Test client unregistration
    # test_post_unregister(client_id)

    # # Test unregistering an unregistered client
    # test_post_unregister_unregistered("unregistered-client")

    # num_clients = 10
    # client_ids = []
    # for i in range(num_clients):
    #     client_ids.append(f"test-id-{i + 1}")

    # # Test POST register for multiple clients
    # for i in range(num_clients):
    #     client_id = client_ids[i]
    #     test_post_register(client_id)
    #     test_get_clients()
    #     start = time.time()
    #     test_init_pipeline(client_id, pipeline_config, force=True)
    #     print(f"init time: {time.time() - start}")

    # async def main():
    #     tasks = []
    #     for client_id in client_ids:
    #         print(f"Start testing WebSocket for client {client_id}.")
    #         task = asyncio.create_task(
    #             test_websocket(
    #                 client_id, process_func, messages, repeat=1, interval=0, timeout=5
    #             )
    #         )
    #         await asyncio.sleep(0.1)
    #         tasks.append(task)
    #     await asyncio.gather(*tasks)

    # asyncio.run(main())
