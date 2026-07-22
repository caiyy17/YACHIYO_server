"""Drive the real webrtc_client/index.html in headless chromium against
the real gateway+pipeline. Chromium's fake-mic device doesn't register in
this environment, so getUserMedia is shimmed via an init script to a
WebAudio stream that loops test_voice.wav (real speech) as the microphone;
the camera path uses the page's own placeholder fallback. Asserts BOTH
sides: the page's protocol behavior (cancel before recording_start on
every Talk press) and the server's handling (vad marks, cancels, a
generated turn, no errors)."""
import base64
from pathlib import Path
import re
import sys
import time

from playwright.sync_api import sync_playwright

PAGE = "http://127.0.0.1:15168/static/index.html"
WAV = Path(__file__).resolve().with_name("test_voice.wav")
FAIL = []


def check(d, c):
    print(("OK   " if c else "FAIL ") + d)
    if not c:
        FAIL.append(d)


SHIM_TEMPLATE = """
const WAV_B64 = "%s";
navigator.mediaDevices.getUserMedia = async (constraints) => {
  if (constraints && constraints.audio) {
    const ctx = new AudioContext();
    await ctx.resume();
    const bytes = Uint8Array.from(atob(WAV_B64), c => c.charCodeAt(0));
    const buf = await ctx.decodeAudioData(bytes.buffer);
    const src = ctx.createBufferSource();
    src.buffer = buf;
    src.loop = true;
    const dst = ctx.createMediaStreamDestination();
    src.connect(dst);
    src.start();
    return dst.stream;
  }
  throw new DOMException('shim: no camera', 'NotFoundError');
};
"""


def main():
    wav_b64 = base64.b64encode(WAV.read_bytes()).decode()
    shim = SHIM_TEMPLATE % wav_b64

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--autoplay-policy=no-user-gesture-required"],
        )
        page = browser.new_context(
            permissions=["microphone", "camera"]
        ).new_page()
        page.add_init_script(shim)
        page.goto(PAGE)
        cid = page.input_value("#clientId")
        print(f"client id: {cid}")

        page.click("#connectBtn")
        page.wait_for_function(
            "document.getElementById('log').textContent.includes('DataChannel open')",
            timeout=120000,
        )
        check("page connected (DataChannel open)", True)
        time.sleep(3)  # steady media flow

        # Turn 1: hold Talk for 2s of looping real speech
        page.dispatch_event("#talkBtn", "mousedown")
        time.sleep(2.0)
        page.dispatch_event("#talkBtn", "mouseup")
        time.sleep(10)  # ASR + LLM + TTS + playback under way

        # Barge-in: press Talk again, hold 1.5s
        page.dispatch_event("#talkBtn", "mousedown")
        time.sleep(1.5)
        page.dispatch_event("#talkBtn", "mouseup")
        time.sleep(8)

        page_log = page.text_content("#log")
        page.click("#disconnectBtn")
        time.sleep(2)
        browser.close()

    events = re.findall(r"Sent: (cancel|recording_start|recording_end)", page_log)
    starts = [i for i, event in enumerate(events) if event == "recording_start"]
    check(
        f"2 presses, each cancel-first {events}",
        len(starts) == 2
        and all(i > 0 and events[i - 1] == "cancel" for i in starts),
    )
    check("page saw a reply (SoS logged)", "SoS" in page_log)

    log = Path(f"logs/client_{cid}.log").read_text()
    check(
        "server got both turns (2 vad start marks)",
        len(re.findall(r"vad start pending|vad mark at sample", log)) == 2,
    )
    check("server processed the cancels", "received cancel signal" in log)
    check("turn generated (response_id minted)", "resp_" in log)
    check("no ERROR in server log", "ERROR" not in log)
    print("ALL PASS" if not FAIL else f"{len(FAIL)} FAILURES")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
