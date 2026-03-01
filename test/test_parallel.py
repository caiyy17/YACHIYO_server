"""
Test for Dispatcher/Receiver parallel execution.

Uses configs/default.json:
  Dispatcher(1) -> FuncA(3, 1s) || FuncB(5, 0.5s) -> Receiver(7)

Requires server running: uvicorn server_fastapi:app --host 0.0.0.0 --port 8000
"""

import json
import requests
import asyncio
import websockets
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SERVER = "http://localhost:8000"
WS_URL = "ws://localhost:8000/ws"


async def run_pipeline(client_id, config_name, messages, timeout=5):
    """Standard pipeline test: register -> init -> ws send -> collect results."""
    requests.post(f"{SERVER}/register/", json={"client_id": client_id})
    requests.post(
        f"{SERVER}/init_pipeline/{client_id}",
        json={"config": config_name, "force": True},
    )
    await asyncio.sleep(1)

    results = []
    start = time.time()
    try:
        async with websockets.connect(
            f"{WS_URL}/{client_id}", max_size=16 * 1024 * 1024
        ) as ws:
            for msg in messages:
                await ws.send(json.dumps(msg))
            while True:
                r = await asyncio.wait_for(ws.recv(), timeout=timeout)
                d = json.loads(r)
                if d.get("signal") and not any(
                    k for k in d if k not in ("signal", "timestamp")
                ):
                    continue
                results.append(d)
                if len(results) >= len(messages):
                    break
    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
        pass

    elapsed = time.time() - start
    requests.post(f"{SERVER}/unregister/", json={"client_id": client_id})
    return results, elapsed


async def test_parallel_execution():
    """FuncA(1s) and FuncB(0.5s) in parallel = ~1s, not ~1.5s."""
    print("=" * 60)
    print("  Test 1: Parallel Execution (default config)")
    print("=" * 60)

    results, elapsed = await run_pipeline(
        "test_par", "default",
        [{"message": "hello", "extra_info": "metadata", "timestamp": time.time()}],
    )

    if not results:
        print("  FAIL: no result")
        return False

    r = results[0]
    has_a = "result_a" in r
    has_b = "result_b" in r
    is_parallel = elapsed < 1.3  # ~1s parallel (FuncA dominates), not 1.5s serial
    has_pass = r.get("message") == "hello"

    print(f"  Elapsed:   {elapsed * 1000:.0f} ms (expect < 1300)")
    print(f"  Branch A:  {r.get('result_a', 'MISSING')}")
    print(f"  Branch B:  {r.get('result_b', 'MISSING')}")
    print(f"  pass_data: message={r.get('message', 'MISSING')}")

    ok = has_a and has_b and is_parallel and has_pass
    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


async def test_temporal_consistency():
    """Two utterances maintain causal order through the parallel bracket."""
    print()
    print("=" * 60)
    print("  Test 2: Temporal Consistency")
    print("=" * 60)

    t1 = time.time()
    t2 = t1 + 1
    results, elapsed = await run_pipeline(
        "test_tc", "default",
        [{"message": "first", "timestamp": t1}, {"message": "second", "timestamp": t2}],
        timeout=10,
    )

    if len(results) < 2:
        print(f"  FAIL: got {len(results)} results, expected 2")
        return False

    order_ok = results[0]["timestamp"] < results[1]["timestamp"]
    complete = all("result_a" in r and "result_b" in r for r in results)

    print(f"  Result 1: ts={results[0]['timestamp']:.2f}")
    print(f"  Result 2: ts={results[1]['timestamp']:.2f}")
    print(f"  Causal order: {order_ok}")
    print(f"  Complete:     {complete}")

    ok = order_ok and complete
    print(f"  {'PASS' if ok else 'FAIL'}")
    return ok


async def main():
    try:
        requests.get(f"{SERVER}/docs", timeout=2)
    except Exception:
        print("Server not running. Start with:")
        print("  uvicorn server_fastapi:app --host 0.0.0.0 --port 8000")
        return False

    r1 = await test_parallel_execution()
    r2 = await test_temporal_consistency()
    print()
    print("=" * 60)
    all_pass = r1 and r2
    print(f"  ALL TESTS: {'PASS' if all_pass else 'FAIL'}")
    print("=" * 60)
    return all_pass


if __name__ == "__main__":
    ok = asyncio.run(main())
    sys.exit(0 if ok else 1)
