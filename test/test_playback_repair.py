"""Playback-repair e2e over every newly wired config topology: real
server, real WS. For each config: direct-send a prompt to the llm node,
collect response_id/item_id FROM THE CLIENT-SIDE WS MESSAGES (proves the
ids actually reach the exit), snapshot the full history, send a
playback_complete report for item 2 (exclusive: keep item 1 only),
assert the history file is cut to a strict prefix + marker, then assert
a stale second report is ignored."""
import asyncio, json, sys, time, uuid
import requests, websockets

MAIN = "http://127.0.0.1:8910"
# config -> (llm node id, prompt wire field)
CASES = {
    "unity_chan_default":  (2, "1_text"),
    "unity_chan_humanoid": (2, "1_text"),
    "unity_chan_live":     (2, "1_prompt"),
    "unity_chan_text":     (1, "text"),
}
PROMPT = "请用三个完整的短句介绍你自己，每句都以句号结尾，不少于三句。"
FAIL = []

def check(d, c):
    print(("  OK   " if c else "  FAIL ") + d)
    if not c: FAIL.append(d)

async def run_case(config, llm_node, field):
    print(f"== {config}")
    cid = f"evt_repair_{uuid.uuid4().hex[:8]}"
    r = requests.post(f"{MAIN}/register/", json={"client_id": cid}, timeout=30)
    r.raise_for_status()
    r = requests.post(f"{MAIN}/init_pipeline/{cid}",
                      json={"config": config}, timeout=180)
    r.raise_for_status()
    try:
        async with websockets.connect(
                f"ws://127.0.0.1:8910/ws/{cid}",
                max_size=16 * 1024 * 1024) as ws:
            await ws.send(json.dumps({field: PROMPT, "destination": llm_node,
                                      "timestamp": time.time()}))
            # collect client-side messages until EoS (ids must be there)
            rid, items = None, []
            deadline = time.time() + 60
            while time.time() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                m = json.loads(raw)
                if m.get("item_id"):
                    rid = m.get("response_id") or rid
                    if m["item_id"] not in items:
                        items.append(m["item_id"])
                if m.get("signal") == "EoS":
                    break
            check(f"client received ids on sentences "
                  f"({len(items)} items, resp={bool(rid)})",
                  rid is not None and len(items) >= 2)
            if not rid or len(items) < 2:
                return
            hist_path = f"history/history_{cid}.json"
            full = json.load(open(hist_path))
            full_content = full[-1]["content"]
            check("history holds full reply pre-report",
                  full[-1]["role"] == "assistant"
                  and "---interrupted---" not in full_content)

            # interrupted while item 2 was playing -> keep item 1 only
            await ws.send(json.dumps({
                "signal": "playback_complete", "timestamp": time.time(),
                "response_id": rid, "item_id": items[1]}))
            await asyncio.sleep(2)
            cut = json.load(open(hist_path))
            cut_content = cut[-1]["content"]
            prefix = cut_content.removesuffix("\n---interrupted---")
            check("history cut to strict prefix + marker",
                  cut_content.endswith("---interrupted---")
                  and prefix and full_content.startswith(prefix)
                  and len(prefix) < len(full_content))

            # repaired turn is closed: stale report ignored
            await ws.send(json.dumps({
                "signal": "playback_complete", "timestamp": time.time(),
                "response_id": rid, "item_id": items[0]}))
            await asyncio.sleep(2)
            check("stale second report ignored",
                  json.load(open(hist_path))[-1]["content"] == cut_content)
            log = open(f"logs/client_{cid}.log").read()
            check("no ERROR in server log", "ERROR" not in log)
    finally:
        requests.post(f"{MAIN}/unregister/", json={"client_id": cid},
                      timeout=30)

async def main():
    for config, (node, field) in CASES.items():
        await run_case(config, node, field)
    print("ALL PASS" if not FAIL else f"{len(FAIL)} FAILURES: {FAIL}")
    sys.exit(1 if FAIL else 0)

asyncio.run(main())
