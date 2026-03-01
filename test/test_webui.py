#!/usr/bin/env python3
"""
Test the WebUI backend API endpoints.

Starts the webui server, tests all API endpoints, then shuts down.
Results saved to test/tmp/test_webui.log (via tee).
"""

import asyncio
import json
import os
import sys
import time

import aiohttp

WEBUI_PORT = 18083  # Use non-default port to avoid conflicts
WEBUI_URL = f"http://localhost:{WEBUI_PORT}"
MAIN_SERVER = "http://localhost:8000"
TEST_CLIENT_ID = "webui_test_client"
PIPELINE_CONFIG = "unity_chan"


async def start_webui():
    """Start webui server as subprocess."""
    import subprocess
    env = os.environ.copy()
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "web_ui:app",
         "--host", "0.0.0.0", "--port", str(WEBUI_PORT)],
        cwd=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "webui"),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env,
    )
    # Wait for server ready
    for _ in range(30):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{WEBUI_URL}/api/configs") as resp:
                    if resp.status == 200:
                        return proc
        except Exception:
            pass
        await asyncio.sleep(0.5)
    proc.kill()
    raise RuntimeError("WebUI server did not start")


async def test_api(session, method, path, json_data=None, expect_success=True):
    """Test a single API endpoint."""
    url = f"{WEBUI_URL}{path}"
    try:
        if method == "GET":
            async with session.get(url) as resp:
                status = resp.status
                body = await resp.json()
        else:
            async with session.post(url, json=json_data) as resp:
                status = resp.status
                body = await resp.json()

        success = body.get("success", False)
        ok = success == expect_success
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {method} {path} -> status={status}, success={success}, msg={body.get('message', '')}")
        return ok, body
    except Exception as e:
        print(f"  [FAIL] {method} {path} -> error: {e}")
        return False, {}


async def main():
    print("=" * 60)
    print("WebUI API Test")
    print(f"  WebUI:  {WEBUI_URL}")
    print(f"  Main:   {MAIN_SERVER}")
    print("=" * 60)

    # Start webui
    print("\nStarting WebUI server...")
    proc = await start_webui()
    print(f"WebUI server started (PID {proc.pid})")

    all_passed = True
    async with aiohttp.ClientSession() as session:
        # 1. Config listing
        print("\n--- Config Management ---")
        ok, body = await test_api(session, "GET", "/api/configs")
        all_passed &= ok
        if ok:
            configs = body.get("data", {}).get("configs", [])
            print(f"    Found {len(configs)} configs")
            config_types = set(c["type"] for c in configs)
            print(f"    Types: {config_types}")

        # 2. View a config
        ok, body = await test_api(session, "GET", "/api/config/config/demo")
        all_passed &= ok
        if ok:
            content = body.get("data", {}).get("content", "")
            is_editable = body.get("data", {}).get("is_editable", False)
            print(f"    demo.json: {len(content)} chars, editable={is_editable}")

        # 3. Client management - register
        print("\n--- Client Management ---")
        ok, _ = await test_api(session, "POST", "/api/register",
                               {"client_id": TEST_CLIENT_ID})
        all_passed &= ok

        # 4. Get client list
        ok, body = await test_api(session, "GET", "/api/clients")
        all_passed &= ok
        if ok:
            clients = body.get("data", {}).get("clients", [])
            has_client = TEST_CLIENT_ID in clients
            print(f"    Clients: {clients}, has test client: {has_client}")
            all_passed &= has_client

        # 5. Get single client
        ok, body = await test_api(session, "GET", f"/api/client/{TEST_CLIENT_ID}")
        all_passed &= ok

        # 6. Init pipeline
        ok, _ = await test_api(session, "POST", f"/api/init_pipeline/{TEST_CLIENT_ID}",
                               {"config": PIPELINE_CONFIG, "force": True})
        all_passed &= ok

        # 7. Get logs
        ok, body = await test_api(session, "GET", f"/api/logs/{TEST_CLIENT_ID}")
        all_passed &= ok
        if ok:
            log_content = body.get("data", {}).get("log_content", "")
            print(f"    Log length: {len(log_content)} chars")

        # 8. Unregister
        ok, _ = await test_api(session, "POST", "/api/unregister",
                               {"client_id": TEST_CLIENT_ID})
        all_passed &= ok

        # 9. Verify client removed
        ok, body = await test_api(session, "GET", "/api/clients")
        all_passed &= ok
        if ok:
            clients = body.get("data", {}).get("clients", [])
            removed = TEST_CLIENT_ID not in clients
            print(f"    Client removed: {removed}")
            all_passed &= removed

        # 10. Error cases
        print("\n--- Error Cases ---")
        # Init pipeline for non-existent client
        ok, _ = await test_api(session, "POST", "/api/init_pipeline/nonexistent",
                               {"config": "demo"}, expect_success=False)
        all_passed &= ok

        # Register without client_id
        ok, _ = await test_api(session, "POST", "/api/register",
                               {}, expect_success=False)
        all_passed &= ok

        # Init pipeline without config
        # First register a client for this test
        await test_api(session, "POST", "/api/register",
                       {"client_id": "error_test"})
        ok, _ = await test_api(session, "POST", "/api/init_pipeline/error_test",
                               {}, expect_success=False)
        all_passed &= ok
        await test_api(session, "POST", "/api/unregister",
                       {"client_id": "error_test"})

        # 11. HTML page loads
        print("\n--- HTML Page ---")
        async with session.get(f"{WEBUI_URL}/") as resp:
            ok = resp.status == 200
            content_type = resp.content_type
            html = await resp.text()
            has_title = "YACHIO" in html
            mark = "PASS" if (ok and has_title) else "FAIL"
            print(f"  [{mark}] GET / -> status={resp.status}, type={content_type}, has_title={has_title}")
            all_passed &= ok and has_title

    # Cleanup
    proc.kill()
    proc.wait()
    print(f"\nWebUI server stopped (PID {proc.pid})")

    print("\n" + "=" * 60)
    if all_passed:
        print("All WebUI tests PASSED!")
    else:
        print("Some WebUI tests FAILED!")
    print("=" * 60)
    return all_passed


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
