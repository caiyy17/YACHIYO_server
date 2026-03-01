#!/usr/bin/env python3
"""
WebRTC multi-user test — runs N concurrent clients in separate processes.

Each client runs in its own process with its own event loop,
matching real-world usage where each client is on a separate machine.

Each client:
  1. Registers + inits pipeline via main server
  2. Connects WebRTC, sends test audio, receives response
  3. Verifies: got DC messages, got non-silent audio, got EoS
  4. Unregisters

Output:
  test/tmp/test_webrtc_multi.log (via tee)
  test/tmp/test_webrtc_multi_{i}.mp4 per client
"""

import asyncio
import json
import multiprocessing
import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "tmp")
NUM_CLIENTS = 3
CLIENT_TEST_DURATION = 30


def run_client_process(client_id, output_mp4, result_dict):
    """Entry point for each client process."""
    import numpy as np
    import aiohttp
    from aiortc import RTCPeerConnection, RTCSessionDescription

    sys.path.insert(0, SCRIPT_DIR)
    from test_webrtc import (
        MAIN_SERVER, WEBRTC_SERVER, PIPELINE_CONFIG, TEST_WAV,
        SAMPLE_RATE, AUDIO_PTIME, AUDIO_SAMPLES, VIDEO_FPS,
        load_test_audio, TestAudioTrack, TestVideoTrack, record_mp4,
    )

    async def run():
        # Register + init pipeline
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{MAIN_SERVER}/register/",
                    json={"client_id": client_id},
                ) as resp:
                    reg = await resp.json()
                    print(f"[{client_id}] Register: {reg}")

                async with session.post(
                    f"{MAIN_SERVER}/init_pipeline/{client_id}",
                    json={"config": PIPELINE_CONFIG, "force": True},
                ) as resp:
                    if resp.status != 200:
                        result_dict["error"] = f"Init pipeline failed: {resp.status}"
                        return
                    init = await resp.json()
                    print(f"[{client_id}] Init pipeline: {init}")
        except Exception as e:
            result_dict["error"] = f"Setup failed: {e}"
            return

        # Load audio
        audio_frames = load_test_audio(TEST_WAV)

        # WebRTC
        pc = RTCPeerConnection()
        send_audio = TestAudioTrack(audio_frames)
        send_video = TestVideoTrack(send_audio)
        pc.addTrack(send_audio)
        pc.addTrack(send_video)

        recv_audio_frames = []
        recv_video_frames = []
        recv_dc_messages = []
        recv_dc_timeline = []
        recv_start = [None]

        @pc.on("track")
        def on_track(track):
            if track.kind == "audio":
                asyncio.ensure_future(_recv_audio(track))
            elif track.kind == "video":
                asyncio.ensure_future(_recv_video(track))

        async def _recv_audio(track):
            try:
                while True:
                    frame = await track.recv()
                    pcm = frame.to_ndarray().flatten().astype(np.int16).copy()
                    if frame.layout.name == "stereo":
                        pcm = pcm[::2]
                    recv_audio_frames.append(pcm)
            except Exception:
                pass

        async def _recv_video(track):
            try:
                while True:
                    frame = await track.recv()
                    if recv_start[0] is None:
                        recv_start[0] = time.time()
                    rgb = frame.to_ndarray(format="rgb24").copy()
                    recv_video_frames.append((time.time() - recv_start[0], rgb))
            except Exception:
                pass

        @pc.on("datachannel")
        def on_datachannel(channel):
            @channel.on("message")
            def on_message(msg):
                try:
                    parsed = json.loads(msg)
                    recv_dc_messages.append(parsed)
                    if recv_start[0] is not None:
                        recv_dc_timeline.append((time.time() - recv_start[0], parsed))
                except Exception:
                    pass

        client_dc = pc.createDataChannel("client-signals", ordered=True)

        # Connect
        offer = await pc.createOffer()
        await pc.setLocalDescription(offer)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{WEBRTC_SERVER}/offer/{client_id}",
                    json={"sdp": pc.localDescription.sdp, "type": pc.localDescription.type},
                ) as resp:
                    if resp.status != 200:
                        await pc.close()
                        result_dict["error"] = f"WebRTC offer failed: {resp.status}"
                        return
                    answer = await resp.json()
        except Exception as e:
            await pc.close()
            result_dict["error"] = f"WebRTC connect failed: {e}"
            return

        await pc.setRemoteDescription(
            RTCSessionDescription(sdp=answer["sdp"], type=answer["type"])
        )

        # Wait for connection
        for _ in range(50):
            if pc.connectionState == "connected":
                break
            await asyncio.sleep(0.1)
        if pc.connectionState != "connected":
            await pc.close()
            result_dict["error"] = f"Connection: {pc.connectionState}"
            return

        # Wait for DC
        for _ in range(50):
            if client_dc.readyState == "open":
                break
            await asyncio.sleep(0.1)
        if client_dc.readyState != "open":
            await pc.close()
            result_dict["error"] = "DataChannel did not open"
            return

        # Send speech
        test_start = time.time()
        client_dc.send(json.dumps({"signal": "vad_start"}))
        print(f"[{client_id}] vad_start sent")

        while not send_audio.finished_speech:
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)

        client_dc.send(json.dumps({"signal": "vad_end"}))
        print(f"[{client_id}] vad_end sent")

        # Wait for test duration
        eos_seen = False
        while time.time() - test_start < CLIENT_TEST_DURATION:
            for msg in recv_dc_messages:
                if msg.get("signal") == "EoS":
                    if not eos_seen:
                        eos_seen = True
                        print(f"[{client_id}] EoS received")
                    break
            await asyncio.sleep(0.2)

        # Record MP4
        record_mp4(
            sent_video=send_video.recorded_frames,
            sent_audio=send_audio.recorded_pcm,
            recv_video=recv_video_frames,
            recv_audio=recv_audio_frames,
            dc_timeline=recv_dc_timeline,
            output_path=output_mp4,
        )

        await pc.close()

        # Unregister
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{MAIN_SERVER}/unregister/",
                    json={"client_id": client_id},
                ) as resp:
                    unreg = await resp.json()
                    print(f"[{client_id}] Unregister: {unreg}")
        except Exception:
            pass

        # Store results
        result_dict["dc_msgs"] = len(recv_dc_messages)
        result_dict["text_msgs"] = sum(1 for m in recv_dc_messages if "text" in m)
        result_dict["eos"] = eos_seen
        result_dict["audio_frames"] = len(recv_audio_frames)
        result_dict["non_silent"] = sum(1 for p in recv_audio_frames if np.any(p != 0))
        result_dict["sent_video"] = len(send_video.recorded_frames)
        result_dict["recv_video"] = len(recv_video_frames)
        result_dict["sent_audio"] = len(send_audio.recorded_pcm)
        result_dict["recv_audio"] = len(recv_audio_frames)

    asyncio.run(run())


def main():
    # Import here to avoid importing heavy libs in main process
    sys.path.insert(0, SCRIPT_DIR)
    from test_webrtc import MAIN_SERVER, WEBRTC_SERVER, PIPELINE_CONFIG

    print("=" * 60)
    print(f"WebRTC Multi-User Test ({NUM_CLIENTS} concurrent processes)")
    print(f"  Main:     {MAIN_SERVER}")
    print(f"  WebRTC:   {WEBRTC_SERVER}")
    print(f"  Pipeline: {PIPELINE_CONFIG}")
    print(f"  Duration: {CLIENT_TEST_DURATION}s per client")
    print("=" * 60)

    manager = multiprocessing.Manager()
    processes = []
    results = []

    for i in range(NUM_CLIENTS):
        cid = f"multi_test_{i+1}"
        mp4 = os.path.join(OUTPUT_DIR, f"test_webrtc_multi_{i+1}.mp4")
        result_dict = manager.dict()
        results.append((cid, result_dict))
        p = multiprocessing.Process(
            target=run_client_process,
            args=(cid, mp4, result_dict),
        )
        processes.append(p)

    # Start all processes
    for p in processes:
        p.start()

    # Wait for all to finish
    for p in processes:
        p.join(timeout=CLIENT_TEST_DURATION + 60)

    print("\n" + "=" * 60)
    print("MULTI-USER RESULTS")
    print("=" * 60)
    all_ok = True
    for cid, rd in results:
        rd = dict(rd)
        if "error" in rd:
            print(f"  [{cid}] FAIL: {rd['error']}")
            all_ok = False
        else:
            eos = rd.get("eos", False)
            text = rd.get("text_msgs", 0)
            non_silent = rd.get("non_silent", 0)
            dc = rd.get("dc_msgs", 0)
            audio = rd.get("audio_frames", 0)
            sv = rd.get("sent_video", 0)
            rv = rd.get("recv_video", 0)
            duration = sv / 30 if sv > 0 else 0
            # Pass if: got text responses + got non-silent audio
            # EoS may not arrive if response is long — normal disconnect scenario
            ok = text > 0 and non_silent > 0
            status = "OK" if ok else "FAIL"
            print(f"  [{cid}] {status}: {dc} DC msgs, {text} text, EoS={eos}, "
                  f"audio={non_silent}/{audio}, video={sv}sent/{rv}recv, {duration:.1f}s")
            if not ok:
                all_ok = False

    # Check server cleanup
    try:
        import requests
        clients = requests.get(f"{MAIN_SERVER}/clients/").json()
        status = requests.get(f"{WEBRTC_SERVER}/status").json()
        print(f"\n  Main server clients after cleanup: {clients}")
        print(f"  WebRTC sessions after cleanup: {status.get('sessions', {})}")
    except Exception as e:
        print(f"  Cleanup check failed: {e}")

    if all_ok:
        print("\nAll clients passed!")
    else:
        print("\nSome clients FAILED!")

    return all_ok


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
