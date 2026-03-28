import time
import json

from ..base.SpanProcessingStep import SpanProcessingStep

# Tolerance for float timestamp comparison (covers JSON serialization precision loss)
TIMESTAMP_EPSILON = 1e-3


class DanmakuBufferStep(SpanProcessingStep):
    """
    Buffers incoming danmaku messages and releases batches to the LLM at intervals.
    Paced by client playback_complete signal.

    Inherits SpanProcessingStep for proper cancel handling during collection spans.

    State machine:
      idle → first danmaku (start_span) → collecting → release (end_span)
        → waiting_for_playback → playback_complete → collecting or idle

    Three independent timers in custom_update:
      1. Playback timeout: from last release wall time. Force-unlock if too long.
      2. Idle talk: from max(last_playback_time, last_cancel_time). Talk when idle.
      3. Max wait: from span_timestamp (first danmaku). Force-release stale buffer.
    """

    GUARD_NAMES = {1: "总督", 2: "提督", 3: "舰长"}

    def span_init(self):
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

        # Pipeline timestamps (for cancel system)
        self.last_release_pts = 0     # last released batch
        self.last_message_pts = 0     # last received message of any kind
        # Wall clocks for timer logic (all 0 = never triggered)
        self.last_release_time = 0   # for playback timeout + release interval
        self.idle_start_time = 0     # for idle talk
        self.span_start_time = 0     # for max_wait (set on first danmaku)

        self.total_released = 0
        self.total_received = 0
        self.total_dropped = 0

    def span_process(self, data, pass_data={}):
        # Any message resets idle timer and records pipeline timestamp
        self.idle_start_time = time.time()
        self.last_message_pts = data.get("timestamp", self.last_message_pts)

        signal = data.get("signal", "")

        # Handle playback_complete signal from client
        if signal == "playback_complete":
            if self.waiting_for_playback:
                client_ts = data.get("last_batch_timestamp", 0)
                if client_ts >= self.last_release_pts - TIMESTAMP_EPSILON:
                    self.waiting_for_playback = False
                    self.logger.info(
                        f"playback_complete matched, unlocked "
                        f"(buf={len(self.buffer)})"
                    )
                else:
                    self.logger.info(
                        f"playback_complete for older batch: "
                        f"client={client_ts}, latest={self.last_release_pts}, "
                        f"staying locked"
                    )
            self.custom_update()
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

        # Start span on first danmaku (if not already collecting)
        if not self.span_active:
            self.start_span(data["timestamp"])
            self.span_start_time = time.time()

        self.buffer.append({
            "text": text,
            "user": user,
            "msg_type": msg_type,
            "priority": priority,
            "timestamp": data["timestamp"],
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

        # Check release conditions (same logic as timeout path)
        self.custom_update()

    def on_span_cancel(self, cancel_message):
        """Cancel during collection: clear buffer."""
        self.buffer = []
        self.waiting_for_playback = False
        self.idle_start_time = time.time()
        self.last_message_pts = cancel_message["timestamp"]
        self.span_start_time = 0
        self.logger.info("span cancelled, buffer cleared")

    def custom_update(self):
        """Release decision logic. Called after each message AND on timeout."""
        now = time.time()

        # Locked: waiting for playback_complete
        if self.waiting_for_playback:
            # Playback timeout: force unlock
            if self.last_release_time > 0 and \
               (now - self.last_release_time) >= self.playback_timeout:
                self.logger.info(
                    f"playback_complete timeout after {self.playback_timeout}s, "
                    f"force unlocking"
                )
                self.waiting_for_playback = False
            else:
                return

        # Unlocked + buffer has content: check release conditions
        if len(self.buffer) > 0:
            should_release = False
            # Enough messages + interval cooldown
            if len(self.buffer) >= self.min_batch_size and self._interval_elapsed():
                should_release = True
            # High priority message (SC, guard, etc.)
            if self._has_high_priority():
                should_release = True
            # Max wait timeout (first danmaku waited too long)
            if (now - self.span_start_time) >= self.max_wait_time:
                should_release = True
            if should_release:
                self._release_batch()
            return

        # Unlocked + buffer empty (guaranteed by return above): idle talk
        if self.idle_start_time > 0 and \
           (now - self.idle_start_time) >= self.idle_talk_interval:
            self._release_idle()

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
        return (time.time() - self.last_release_time) >= self.release_interval

    def _release_batch(self):
        """Format and release a batch of danmaku to the LLM."""
        sorted_buf = sorted(
            self.buffer, key=lambda x: (-x["priority"], x["timestamp"])
        )
        batch = sorted_buf[: self.max_batch_size]
        batch_set = set(id(m) for m in batch)
        self.buffer = [m for m in self.buffer if id(m) not in batch_set]

        prompt = self._format_batch(batch)

        # Use span start timestamp (first danmaku), consistent with vad_start pattern
        ts = self.current_timestamp or self.last_release_pts

        output_data = {}
        self.add_output(output_data, "prompt", prompt)
        self.output_to_queue(output_data, {"timestamp": ts})

        self.last_release_pts = ts
        self.last_release_time = time.time()
        self.waiting_for_playback = True
        self.total_released += len(batch)

        # End span after release; new span starts when next danmaku arrives
        self.end_span()

        self.logger.info(
            f"released batch: {len(batch)} msgs, ts={ts}, "
            f"remaining={len(self.buffer)}, "
            f"total_recv={self.total_received}, "
            f"total_released={self.total_released}"
        )

    def _release_idle(self):
        """Send an empty prompt so LLM can decide to talk on its own."""
        prompt = "（当前没有新弹幕）"
        output_data = {}
        self.add_output(output_data, "prompt", prompt)
        self.output_to_queue(output_data, {"timestamp": self.last_message_pts})

        self.last_release_pts = self.last_message_pts
        self.last_release_time = time.time()
        self.waiting_for_playback = True
        self.logger.info(f"released idle prompt, pts={self.last_release_pts}")

    def _format_batch(self, batch):
        """Format batch with section separation and duplicate merging."""
        special = []
        regular = []

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

        special.sort(key=lambda x: x[1])
        special_lines = [line for line, _ in special]

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

        sections = []
        if regular_lines:
            sections.append("===观众弹幕===\n" + "\n".join(regular_lines))
        if special_lines:
            sections.append("===系统通知===\n" + "\n".join(special_lines))
        return "\n\n".join(sections) if sections else "(无弹幕)"
