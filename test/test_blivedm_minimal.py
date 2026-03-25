"""
Minimal blivedm test: connect to a random VTuber room and print danmaku for 60 seconds.
"""
import asyncio
import aiohttp
import random
import time
import uuid

import blivedm
import blivedm.models.web as web_models


BILIBILI_ROOM_LIST_API = "https://api.live.bilibili.com/room/v1/area/getRoomList"

BILIBILI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://live.bilibili.com/",
    "Origin": "https://live.bilibili.com",
}


def create_bilibili_session():
    """Create aiohttp session with headers and cookies to pass Bilibili anti-crawler."""
    cookies = {"buvid3": str(uuid.uuid4()) + "infoc"}
    return aiohttp.ClientSession(headers=BILIBILI_HEADERS, cookies=cookies)


async def find_active_vtuber_room(session):
    """Find an active VTuber live room (area_id=371, parent_area_id=9)."""
    params = {
        "platform": "web",
        "parent_area_id": 9,
        "cate_id": 0,
        "area_id": 371,
        "sort_type": "online",
        "page": 1,
        "page_size": 20,
    }
    async with session.get(BILIBILI_ROOM_LIST_API, params=params) as resp:
        data = await resp.json()
        rooms = data.get("data", [])
        if not rooms:
            print("No active VTuber rooms found!")
            return None, None, None
        room = random.choice(rooms[:min(10, len(rooms))])
        return room["roomid"], room["title"], room["uname"]


class TestHandler(blivedm.BaseHandler):
    def __init__(self):
        self.msg_count = 0

    def _on_danmaku(self, client, message: web_models.DanmakuMessage):
        self.msg_count += 1
        print(f"[弹幕] {message.uname}: {message.msg}")

    def _on_gift(self, client, message: web_models.GiftMessage):
        self.msg_count += 1
        print(f"[礼物] {message.uname} 送了 {message.gift_name} x{message.num}")

    def _on_super_chat(self, client, message: web_models.SuperChatMessage):
        self.msg_count += 1
        print(f"[SC] {message.uname} (¥{message.price}): {message.message}")

    def _on_buy_guard(self, client, message: web_models.GuardBuyMessage):
        self.msg_count += 1
        print(f"[舰长] {message.uname} 开通了舰长 (level {message.guard_level})")


async def main():
    session = create_bilibili_session()
    try:
        print("Searching for active VTuber rooms...")
        room_id, title, uname = await find_active_vtuber_room(session)
        if room_id is None:
            return

        print(f"\nConnecting to room {room_id}: {uname} - {title}")
        print(f"Listening for 60 seconds...\n{'='*60}")

        handler = TestHandler()
        client = blivedm.BLiveClient(room_id, session=session)
        client.set_handler(handler)
        client.start()

        try:
            await asyncio.sleep(60)
        finally:
            client.stop()
            await client.join()

        print(f"\n{'='*60}")
        print(f"Total messages received: {handler.msg_count}")
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
