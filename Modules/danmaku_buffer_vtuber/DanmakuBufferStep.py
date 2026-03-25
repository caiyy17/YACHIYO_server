import time
import json

from ..base.BaseProcessingStep import BaseProcessingStep


class DanmakuBufferStep(BaseProcessingStep):
    """
    Buffers incoming danmaku messages and releases batches to the LLM at intervals.

    Uses standard process() to buffer each incoming message,
    and custom_update() (called when queue is empty) for timer-based batch release.

    Priority system:
      gift=10, guard=9, super_chat=8, @character=7, question=6, regular=3, spam=1
    """

    GUARD_NAMES = {1: "总督", 2: "提督", 3: "舰长"}

    def custom_init(self):
        self.buffer = []
        self.release_interval = self.get_config("release_interval", 12)
        self.max_batch_size = self.get_config("max_batch_size", 8)
        self.min_batch_size = self.get_config("min_batch_size", 2)
        self.max_wait_time = self.get_config("max_wait_time", 30)
        self.max_buffer_size = self.get_config("max_buffer_size", 50)
        self.character_name = self.get_config("character_name", "优酱")
        self.last_release_time = time.time()
        self.total_released = 0
        self.total_received = 0
        self.total_dropped = 0

    def process(self, data, pass_data={}):
        """Buffer each incoming danmaku message and check release conditions."""
        text = data.get("text", "")
        user = data.get("user", "unknown")
        msg_type = data.get("msg_type", "danmaku")
        price_yuan = data.get("price_yuan", 0)
        priority = self._classify_priority(text, msg_type, price_yuan)

        self.buffer.append({
            "text": text,
            "user": user,
            "msg_type": msg_type,
            "priority": priority,
            "timestamp": time.time(),
            # Extra fields for formatting
            "guard_level": data.get("guard_level", 0),
            "gift_num": data.get("gift_num", 1),
            "price_yuan": data.get("price_yuan", 0),
        })
        self.total_received += 1

        # Evict lowest-priority messages if buffer overflows
        if len(self.buffer) > self.max_buffer_size:
            self.buffer.sort(key=lambda x: (-x["priority"], x["timestamp"]))
            dropped = len(self.buffer) - self.max_buffer_size
            self.buffer = self.buffer[: self.max_buffer_size]
            self.total_dropped += dropped

        self.logger.info(
            f"buffered [{msg_type}] {user}: {text} "
            f"(priority={priority}, buf={len(self.buffer)})"
        )

        # High-priority messages trigger immediate release
        if priority >= 8:
            self._release_batch(pass_data)
        # Normal release: enough messages + interval elapsed
        elif len(self.buffer) >= self.min_batch_size and self._interval_elapsed():
            self._release_batch(pass_data)

    def custom_update(self):
        """Called when no new messages. Handle max_wait_time fallback release."""
        if len(self.buffer) > 0 and self._max_wait_elapsed():
            self._release_batch({"timestamp": time.time()})
        elif len(self.buffer) == 0:
            # Reset timer when buffer is empty so idle time doesn't count
            self.last_release_time = time.time()

    def _classify_priority(self, text, msg_type, price_yuan=0):
        if msg_type == "gift":
            return 10 if price_yuan > 0 else 3  # Free gifts = normal priority
        if msg_type == "guard":
            return 9
        if msg_type == "super_chat":
            return 8
        if len(text) < 3:
            return 1
        # Single emote messages (B站表情包)
        if text.startswith("[") and text.endswith("]") and text.count("[") <= 2:
            return 1
        # Common single-word reactions
        if text in ("流汗", "流口水", "吃草", "笑死", "哈哈", "666", "好耶", "草"):
            return 1
        if self.character_name in text:
            return 7
        if "?" in text or "？" in text:
            return 6
        return 3

    def _interval_elapsed(self):
        return (time.time() - self.last_release_time) >= self.release_interval

    def _max_wait_elapsed(self):
        return (time.time() - self.last_release_time) >= self.max_wait_time

    def _release_batch(self, pass_data):
        """Format and release a batch of danmaku to the LLM."""
        sorted_buf = sorted(
            self.buffer, key=lambda x: (-x["priority"], x["timestamp"])
        )
        batch = sorted_buf[: self.max_batch_size]
        batch_set = set(id(m) for m in batch)
        self.buffer = [m for m in self.buffer if id(m) not in batch_set]

        prompt = self._format_batch(batch)

        output_data = {}
        self.add_output(output_data, "prompt", prompt)
        self.output_to_queue(output_data, pass_data)

        self.last_release_time = time.time()
        self.total_released += len(batch)
        self.logger.info(
            f"released batch: {len(batch)} msgs, "
            f"remaining={len(self.buffer)}, "
            f"total_recv={self.total_received}, "
            f"total_released={self.total_released}"
        )

    def _format_batch(self, batch):
        """Format batch with tagged identities and duplicate merging.

        Every message gets an explicit identity tag so LLM can distinguish:
        【礼物】大佬甲 送了 小花花 x10
        【SC ¥50】土豪君: 想听唱歌
        【上舰】新舰长 开通了舰长
        【舰长】老舰长: 今天好有趣
        【普通用户】路人A: 我开通了舰长    ← clearly fake
        【多用户 ×5】唱歌！               ← merged duplicates
        """
        special_lines = []
        regular = []  # (text, user, identity_tag)

        for msg in batch:
            if msg["msg_type"] == "gift":
                price = msg.get("price_yuan", 0)
                num = msg.get("gift_num", 1)
                if price > 0:
                    special_lines.append(
                        f"【礼物 ¥{price:.0f}】{msg['user']} 送了 {msg['text']} x{num}"
                    )
                else:
                    # Free gifts treated as regular messages
                    regular.append((f"送了{msg['text']}", msg["user"], "普通用户"))
            elif msg["msg_type"] == "guard":
                guard_name = self.GUARD_NAMES.get(
                    msg.get("guard_level", 3), "舰长"
                )
                special_lines.append(
                    f"【上舰】{msg['user']} 开通了{guard_name}"
                )
            elif msg["msg_type"] == "super_chat":
                price = msg.get("price_yuan", 0)
                special_lines.append(
                    f"【SC ¥{price:.0f}】{msg['user']}: {msg['text']}"
                )
            else:
                guard_level = msg.get("guard_level", 0)
                if guard_level > 0:
                    tag = self.GUARD_NAMES.get(guard_level, "舰长")
                else:
                    tag = "普通用户"
                regular.append((msg["text"], msg["user"], tag))

        # Merge regular danmaku with same text
        from collections import OrderedDict
        merged = OrderedDict()  # text -> [(user, tag)]
        for text, user, tag in regular:
            if text not in merged:
                merged[text] = []
            merged[text].append((user, tag))

        regular_lines = []
        for text, user_list in merged.items():
            if len(user_list) == 1:
                user, tag = user_list[0]
                if tag == "普通用户":
                    regular_lines.append(f"{user}: {text}")
                else:
                    regular_lines.append(f"【{tag}】{user}: {text}")
            else:
                regular_lines.append(f"(×{len(user_list)}) {text}")

        # Build final prompt with clear section separation
        sections = []
        if special_lines:
            sections.append("===系统通知===\n" + "\n".join(special_lines))
        if regular_lines:
            sections.append("===观众弹幕===\n" + "\n".join(regular_lines))
        return "\n\n".join(sections) if sections else "(无弹幕)"
