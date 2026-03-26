import time
import json

from ..base.BaseProcessingStep import BaseProcessingStep


class DanmakuBufferStep(BaseProcessingStep):
    """
    Buffers incoming danmaku messages and releases batches to the LLM at intervals.
    Paced by client playback_complete signal.

    Uses standard process() to buffer each incoming message,
    and custom_update() (called when queue is empty) for timer-based batch release.

    State machine:
      idle → release batch → waiting_for_playback → receive playback_complete
        → buffer has msgs? → release batch (loop)
        → buffer empty? → idle timer starts → idle timeout → release idle (loop)
    """

    GUARD_NAMES = {1: "总督", 2: "提督", 3: "舰长"}

    def custom_init(self):
        self.catch_signal_set = {"playback_complete"}

        self.buffer = []
        self.release_interval = self.get_config("release_interval", 12)
        self.max_batch_size = self.get_config("max_batch_size", 8)
        self.min_batch_size = self.get_config("min_batch_size", 2)
        self.max_wait_time = self.get_config("max_wait_time", 30)
        self.max_buffer_size = self.get_config("max_buffer_size", 50)
        self.idle_talk_interval = self.get_config("idle_talk_interval", 45)
        self.playback_timeout = self.get_config("playback_timeout", 60)
        self.character_name = self.get_config("character_name", "优酱")

        # Playback pacing state
        self.waiting_for_playback = False
        self.last_batch_timestamp = 0  # timestamp of the last released batch
        self._last_release_wall_time = 0  # wall clock of last release (0 = never released)
        self.idle_start_time = time.time()  # start idle timer immediately

        self.total_released = 0
        self.total_received = 0
        self.total_dropped = 0

    def process(self, data, pass_data={}):
        signal = data.get("signal", "")

        # Handle playback_complete signal from client
        if signal == "playback_complete":
            client_ts = data.get("last_batch_timestamp", 0)
            if client_ts >= self.last_batch_timestamp:
                # Matches latest batch — unlock
                self.waiting_for_playback = False
                self.idle_start_time = time.time()
                self.logger.info(
                    f"playback_complete matched, unlocked "
                    f"(buf={len(self.buffer)})"
                )
                if self._should_release_now():
                    self._release_batch(pass_data)
            else:
                # Older batch completed, latest batch still playing — stay locked
                self.logger.info(
                    f"playback_complete for older batch: "
                    f"client={client_ts}, latest={self.last_batch_timestamp}, "
                    f"staying locked"
                )
            return

        # Normal danmaku message — buffer it
        text = data.get("text", "")
        user = data.get("user", "unknown")
        msg_type = data.get("msg_type", "danmaku")
        price = data.get("price", 0)
        priority = self._classify_priority(text, msg_type, price)

        # Drop emote-only and single-reaction messages
        if priority <= 1:
            return

        self.buffer.append({
            "text": text,
            "user": user,
            "msg_type": msg_type,
            "priority": priority,
            "timestamp": time.time(),
            "guard_level": data.get("guard_level", 0),
            "num": data.get("num", 0),
            "price": price,
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

        # Only release if not waiting for playback
        if not self.waiting_for_playback:
            if len(self.buffer) >= self.min_batch_size and self._interval_elapsed():
                self._release_batch(pass_data)

    def custom_update(self):
        """Called when no new messages. Handle timeouts and idle talk."""
        now = time.time()

        if self.waiting_for_playback:
            # Timeout: client didn't send playback_complete in time
            if (now - self._last_release_wall_time) >= self.playback_timeout:
                self.logger.info(
                    f"playback_complete timeout after {self.playback_timeout}s, "
                    f"force unlocking"
                )
                self.waiting_for_playback = False
                self.idle_start_time = now
            return

        # Not waiting — check if we should release
        if len(self.buffer) > 0 and self._max_wait_elapsed():
            self._release_batch({"timestamp": self.last_batch_timestamp})
        elif len(self.buffer) == 0 and self.idle_start_time is not None:
            if (now - self.idle_start_time) >= self.idle_talk_interval:
                self._release_idle({"timestamp": self.last_batch_timestamp})

    def _should_release_now(self):
        """Check if buffer has enough to release right after playback_complete."""
        if len(self.buffer) == 0:
            return False
        if self._has_high_priority():
            return True
        if len(self.buffer) >= self.min_batch_size:
            return True
        return False

    def _has_high_priority(self):
        return any(m["priority"] >= 8 for m in self.buffer)

    def _classify_priority(self, text, msg_type, price=0):
        if msg_type == "gift":
            return 10 if price > 0 else 3
        if msg_type == "guard":
            return 9
        if msg_type == "super_chat":
            return 8
        if len(text) < 3:
            return 1
        if text.startswith("[") and text.endswith("]") and text.count("[") <= 2:
            return 1
        if text in ("流汗", "流口水", "吃草", "笑死", "哈哈", "666", "好耶", "草"):
            return 1
        if self.character_name in text:
            return 7
        if "?" in text or "？" in text:
            return 6
        return 3

    def _interval_elapsed(self):
        return (time.time() - self._last_release_wall_time) >= self.release_interval

    def _max_wait_elapsed(self):
        return (time.time() - self._last_release_wall_time) >= self.max_wait_time

    def _release_batch(self, pass_data):
        """Format and release a batch of danmaku to the LLM."""
        sorted_buf = sorted(
            self.buffer, key=lambda x: (-x["priority"], x["timestamp"])
        )
        batch = sorted_buf[: self.max_batch_size]
        batch_set = set(id(m) for m in batch)
        self.buffer = [m for m in self.buffer if id(m) not in batch_set]

        prompt = self._format_batch(batch)

        # Use the latest message's timestamp in this batch
        ts = max(m["timestamp"] for m in batch) if batch else pass_data.get("timestamp", self.last_batch_timestamp)

        output_data = {}
        self.add_output(output_data, "prompt", prompt)
        self.output_to_queue(output_data, {"timestamp": ts})

        self.last_batch_timestamp = ts
        self._last_release_wall_time = time.time()
        self.waiting_for_playback = True
        self.idle_start_time = None
        self.total_released += len(batch)
        self.logger.info(
            f"released batch: {len(batch)} msgs, ts={ts}, "
            f"remaining={len(self.buffer)}, "
            f"total_recv={self.total_received}, "
            f"total_released={self.total_released}"
        )

    def _release_idle(self, pass_data):
        """Send an empty prompt so LLM can decide to talk on its own."""
        prompt = "（当前没有新弹幕）"
        output_data = {}
        self.add_output(output_data, "prompt", prompt)
        self.output_to_queue(output_data, pass_data)

        self._last_release_wall_time = time.time()
        self.waiting_for_playback = True
        self.idle_start_time = None
        self.logger.info(f"released idle prompt, ts={self.last_batch_timestamp}")

    def _format_batch(self, batch):
        """Format batch with section separation and duplicate merging."""
        special = []  # (line, price) for sorting by price ascending (most expensive last)
        regular = []  # (text, user, identity_tag)

        for msg in batch:
            if msg["msg_type"] == "gift":
                price = msg.get("price", 0)
                num = msg.get("num", 0)
                if price > 0:
                    special.append((
                        f"【礼物 ¥{price:.0f}】{msg['user']} 送了 {msg['text']} x{num}",
                        price,
                    ))
                else:
                    regular.append((f"送了{msg['text']}", msg["user"], "普通用户"))
            elif msg["msg_type"] == "guard":
                guard_name = self.GUARD_NAMES.get(
                    msg.get("guard_level", 3), "舰长"
                )
                price = msg.get("price", 0)
                num = msg.get("num", 0)
                months = f" {num}个月" if num > 1 else ""
                special.append((
                    f"【上舰 ¥{price:.0f}】{msg['user']} 开通了{guard_name}{months}",
                    price,
                ))
            elif msg["msg_type"] == "super_chat":
                price = msg.get("price", 0)
                special.append((
                    f"【SC ¥{price:.0f}】{msg['user']}: {msg['text']}",
                    price,
                ))
            else:
                guard_level = msg.get("guard_level", 0)
                if guard_level > 0:
                    tag = self.GUARD_NAMES.get(guard_level, "舰长")
                else:
                    tag = "普通用户"
                regular.append((msg["text"], msg["user"], tag))

        # Sort special by price ascending (most expensive = most important = last)
        special.sort(key=lambda x: x[1])
        special_lines = [line for line, _ in special]

        # Merge regular danmaku with same text
        from collections import OrderedDict
        merged = OrderedDict()
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

        # Order: regular danmaku first, system notifications last (most important at end)
        sections = []
        if regular_lines:
            sections.append("===观众弹幕===\n" + "\n".join(regular_lines))
        if special_lines:
            sections.append("===系统通知===\n" + "\n".join(special_lines))
        return "\n\n".join(sections) if sections else "(无弹幕)"
