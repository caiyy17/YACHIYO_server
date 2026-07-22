#!/usr/bin/env python3
"""Integration test for the WebUI backend API."""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

import aiohttp


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEBUI_DIR = os.path.join(PROJECT_ROOT, "webui")
WEBUI_HOST = "127.0.0.1"
WEBUI_PORT = 18083
WEBUI_URL = f"http://{WEBUI_HOST}:{WEBUI_PORT}"
MAIN_SERVER = "http://127.0.0.1:8910"

PIPELINE_CONFIG = "loopback"
TEST_CLIENT_ID = f"webui_test_{os.getpid()}_{time.time_ns()}"

STARTUP_TIMEOUT = 15
PROCESS_STOP_TIMEOUT = 10
STARTUP_LOG_TAIL_BYTES = 32 * 1024
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=5)
PROBE_TIMEOUT = aiohttp.ClientTimeout(total=1, connect=1)
UNSET = object()


def assert_port_available():
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((WEBUI_HOST, WEBUI_PORT))
        except OSError as e:
            raise RuntimeError(f"WebUI test port {WEBUI_PORT} is already in use") from e


def assert_webui_process(proc):
    if proc.poll() is not None:
        raise RuntimeError(f"WebUI process exited early with code {proc.returncode}")


def close_startup_log(log_file, show_tail=False):
    log_path = log_file.name
    try:
        if show_tail:
            log_file.flush()
            log_file.seek(0, os.SEEK_END)
            size = log_file.tell()
            log_file.seek(max(0, size - STARTUP_LOG_TAIL_BYTES))
            tail = log_file.read().decode("utf-8", errors="replace").strip()
            if tail:
                print("WebUI startup log tail:", file=sys.stderr)
                print(tail, file=sys.stderr)
    finally:
        log_file.close()
        try:
            os.unlink(log_path)
        except FileNotFoundError:
            pass


async def stop_webui(proc, log_file, require_clean=True, show_log=False):
    try:
        if proc.poll() is None:
            proc.terminate()
        try:
            returncode = await asyncio.to_thread(proc.wait, PROCESS_STOP_TIMEOUT)
        except subprocess.TimeoutExpired as e:
            proc.kill()
            await asyncio.to_thread(proc.wait, PROCESS_STOP_TIMEOUT)
            raise RuntimeError("WebUI process did not stop before the deadline") from e

        if proc.poll() is None:
            raise RuntimeError("WebUI process was not reaped")
        if require_clean and returncode not in (0, -signal.SIGTERM):
            raise RuntimeError(f"WebUI process exited with code {returncode}")
        print(f"WebUI process {proc.pid} exited with code {returncode}")
    finally:
        close_startup_log(log_file, show_tail=show_log)


async def start_webui():
    assert_port_available()
    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "web_ui:app",
        "--host",
        WEBUI_HOST,
        "--port",
        str(WEBUI_PORT),
        "--log-level",
        "warning",
    ]
    log_file = tempfile.NamedTemporaryFile(
        mode="w+b", prefix="yachiyo_webui_test_", suffix=".log", delete=False
    )
    try:
        proc = subprocess.Popen(
            command,
            cwd=WEBUI_DIR,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
    except Exception:
        close_startup_log(log_file)
        raise

    deadline = asyncio.get_running_loop().time() + STARTUP_TIMEOUT
    last_error = "connection not ready"
    try:
        async with aiohttp.ClientSession(timeout=PROBE_TIMEOUT) as session:
            while asyncio.get_running_loop().time() < deadline:
                assert_webui_process(proc)
                try:
                    async with session.get(
                        f"{WEBUI_URL}/api/configs", timeout=PROBE_TIMEOUT
                    ) as response:
                        text = await response.text()
                    if response.status != 200:
                        raise RuntimeError(
                            f"WebUI readiness returned HTTP {response.status}: {text}"
                        )
                    body = json.loads(text)
                    if body.get("success") is not True:
                        raise RuntimeError(f"Invalid WebUI readiness response: {body}")
                    await asyncio.sleep(0.1)
                    assert_webui_process(proc)
                    return proc, log_file
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    last_error = str(e)
                await asyncio.sleep(0.1)
        raise RuntimeError(
            f"WebUI did not start within {STARTUP_TIMEOUT}s: {last_error}"
        )
    except Exception:
        await stop_webui(
            proc, log_file, require_clean=False, show_log=True
        )
        raise


async def request_json(session, base_url, method, path, json_data=UNSET):
    kwargs = {}
    if json_data is not UNSET:
        kwargs["json"] = json_data
    async with session.request(
        method,
        f"{base_url}{path}",
        timeout=REQUEST_TIMEOUT,
        **kwargs,
    ) as response:
        text = await response.text()
        status = response.status
    try:
        body = json.loads(text)
    except json.JSONDecodeError as e:
        raise AssertionError(f"{method} {path} returned non-JSON: {text!r}") from e
    if not isinstance(body, dict):
        raise AssertionError(f"{method} {path} returned non-object JSON: {body!r}")
    return status, body


def decode_detail(detail):
    if isinstance(detail, str):
        try:
            return json.loads(detail)
        except json.JSONDecodeError:
            pass
    return detail


async def test_api(
    session,
    method,
    path,
    *,
    json_data=UNSET,
    status,
    success=UNSET,
    detail=UNSET,
    message=UNSET,
):
    actual_status, body = await request_json(
        session, WEBUI_URL, method, path, json_data
    )
    assert actual_status == status, (method, path, actual_status, body)
    if success is not UNSET:
        assert body.get("success", UNSET) is success, (method, path, body)
    if detail is not UNSET:
        assert decode_detail(body.get("detail", UNSET)) == detail, (
            method,
            path,
            body,
        )
    if message is not UNSET:
        assert body.get("message", UNSET) == message, (method, path, body)
    if success is UNSET and detail is UNSET:
        raise AssertionError("Every API case must assert success or detail")
    print(f"  [PASS] {method} {path} -> HTTP {actual_status}")
    return body


async def cleanup_client(session):
    status, before = await request_json(session, MAIN_SERVER, "GET", "/clients/")
    assert status == 200 and isinstance(before.get("clients"), list), before
    was_registered = TEST_CLIENT_ID in before["clients"]

    status, body = await request_json(
        session,
        MAIN_SERVER,
        "POST",
        "/unregister/",
        {"client_id": TEST_CLIENT_ID},
    )
    expected = "unregistered" if was_registered else "not registered"
    assert status == 200, body
    assert body == {"status": expected, "client_id": TEST_CLIENT_ID}, body

    status, after = await request_json(session, MAIN_SERVER, "GET", "/clients/")
    assert status == 200 and TEST_CLIENT_ID not in after.get("clients", []), after
    try:
        os.unlink(os.path.join(PROJECT_ROOT, "logs", f"client_{TEST_CLIENT_ID}.log"))
    except FileNotFoundError:
        pass
    print(f"Client cleanup verified ({expected})")


async def run_tests(session):
    status, main_clients = await request_json(
        session, MAIN_SERVER, "GET", "/clients/"
    )
    assert status == 200 and isinstance(main_clients.get("clients"), list)

    print("\n--- Configuration and page ---")
    body = await test_api(
        session, "GET", "/api/configs", status=200, success=True, message="Success"
    )
    configs = body["data"]["configs"]
    assert {
        "name": PIPELINE_CONFIG,
        "type": "config",
        "path": "configs",
    } in configs

    body = await test_api(
        session,
        "GET",
        f"/api/config/config/{PIPELINE_CONFIG}",
        status=200,
        success=True,
        message="Success",
    )
    assert body["data"]["config_name"] == PIPELINE_CONFIG
    assert body["data"]["config_type"] == "config"
    assert isinstance(json.loads(body["data"]["content"]), dict)

    async with session.get(f"{WEBUI_URL}/", timeout=REQUEST_TIMEOUT) as response:
        html = await response.text()
        assert response.status == 200
        assert response.content_type == "text/html"
        assert "YACHIYO" in html
    print("  [PASS] GET / -> HTTP 200 HTML")

    print("\n--- Negative cases before registration ---")
    await test_api(
        session,
        "GET",
        f"/api/logs/{TEST_CLIENT_ID}",
        status=404,
        detail={
            "detail": {
                "error": "client log not found",
                "client_id": TEST_CLIENT_ID,
            }
        },
    )
    await test_api(
        session,
        "POST",
        f"/api/init_pipeline/{TEST_CLIENT_ID}",
        json_data={"config": PIPELINE_CONFIG, "force": False},
        status=404,
        detail={"detail": "Client not found"},
    )
    await test_api(
        session,
        "POST",
        "/api/register",
        json_data={},
        status=400,
        detail="Client ID is required",
    )

    print("\n--- Client lifecycle ---")
    body = await test_api(
        session,
        "POST",
        "/api/register",
        json_data={"client_id": TEST_CLIENT_ID},
        status=200,
        success=True,
        message="Registered",
    )
    assert body["data"] == {"status": "registered", "client_id": TEST_CLIENT_ID}

    body = await test_api(
        session, "GET", "/api/clients", status=200, success=True, message="Success"
    )
    assert TEST_CLIENT_ID in body["data"]["clients"]

    body = await test_api(
        session,
        "GET",
        f"/api/client/{TEST_CLIENT_ID}",
        status=200,
        success=True,
        message="Success",
    )
    assert body["data"] == {"status": "connected", "client_id": TEST_CLIENT_ID}

    body = await test_api(
        session,
        "GET",
        f"/api/logs/{TEST_CLIENT_ID}",
        status=200,
        success=True,
        message="Logs retrieved",
    )
    assert TEST_CLIENT_ID in body["data"]["log_content"]

    await test_api(
        session,
        "POST",
        f"/api/init_pipeline/{TEST_CLIENT_ID}",
        json_data={},
        status=400,
        detail="Config name is required",
    )
    body = await test_api(
        session,
        "POST",
        f"/api/init_pipeline/{TEST_CLIENT_ID}",
        json_data={"config": PIPELINE_CONFIG, "force": False},
        status=200,
        success=True,
        message="Pipeline initialized",
    )
    assert body["data"] == {"status": "initialized", "client_id": TEST_CLIENT_ID}

    body = await test_api(
        session,
        "POST",
        "/api/unregister",
        json_data={"client_id": TEST_CLIENT_ID},
        status=200,
        success=True,
        message="Unregistered",
    )
    assert body["data"] == {"status": "unregistered", "client_id": TEST_CLIENT_ID}

    body = await test_api(
        session, "GET", "/api/clients", status=200, success=True, message="Success"
    )
    assert TEST_CLIENT_ID not in body["data"]["clients"]


async def main():
    print("WebUI API integration test")
    print(f"  WebUI: {WEBUI_URL}")
    print(f"  Main:  {MAIN_SERVER}")
    print(f"  Client: {TEST_CLIENT_ID}")

    proc, log_file = await start_webui()
    print(f"WebUI process verified (PID {proc.pid})")
    try:
        async with aiohttp.ClientSession(timeout=REQUEST_TIMEOUT) as session:
            try:
                await run_tests(session)
            finally:
                await cleanup_client(session)
    finally:
        await stop_webui(proc, log_file)
    print("\nAll WebUI tests PASSED")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"\nWebUI test FAILED: {e}", file=sys.stderr)
        sys.exit(1)
