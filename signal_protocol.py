"""Timestamped reference signals, terminal cancellations and an audit outbox.

SQLite survives worker restarts on the same filesystem, not Render redeploys.
No broker orders are placed and reference outcomes are never actual fills.
"""

import json
import math
import os
import sqlite3
import threading
import time
import uuid
import fcntl
import hashlib
from contextlib import contextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

JST = ZoneInfo("Asia/Tokyo")
MAX_ENTRY_AGE = 5
PREVIEW_SECONDS = 60
EXPIRY_SECONDS = 300
RESULT_GRACE_SECONDS = 90


def stamp(seconds):
    return datetime.fromtimestamp(seconds, JST).strftime("%Y/%m/%d %H:%M:%S JST")


def number(data, key):
    value = float(data[key])
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"invalid {key}")
    return value


def validate(data):
    if data.get("schema_version") != 2 or data.get("validation") is not True:
        raise ValueError("schema_version 2 and validation=true required")
    pair = str(data.get("pair", "")).replace("/", "")
    direction = data.get("signal")
    ticker = str(data.get("ticker", ""))
    if (pair not in ("USDJPY", "BTCUSD") or direction not in ("HIGH", "LOW")
            or str(data.get("timeframe")) != "1" or ticker.split(":")[-1] != pair):
        raise ValueError("unsupported instrument, direction or timeframe")
    notice = data.get("notice")
    if notice not in ("PRE_ENTRY", "ENTRY", "PRE_ENTRY_CANCEL", "REFERENCE_RESULT"):
        raise ValueError("unsupported notice")
    entry_ms = int(number(data, "entry_time_ms"))
    event_id = f"{ticker}:{entry_ms}:{direction}"
    if data.get("event_id") != event_id or len(event_id) > 150:
        raise ValueError("event_id mismatch")
    event = dict(data, pair=pair, entry_time_ms=entry_ms)
    event["signal_price"] = number(data, "signal_price")
    event["emitted_ms"] = number(data, "emitted_ms")
    event["preview_started_ms"] = float(data.get("preview_started_ms", 0))
    if not math.isfinite(event["preview_started_ms"]) or event["preview_started_ms"] < 0:
        raise ValueError("invalid preview timestamp")
    if notice == "REFERENCE_RESULT":
        event["result_price"] = number(data, "result_price")
        event["result_time_ms"] = number(data, "result_time_ms")
    return event


class ReferenceSignals:
    def __init__(self, path, send, archive=None, clock=time.time):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path, self.send, self.archive, self.clock = path, send, archive, clock
        self.locks = [threading.RLock() for _ in range(1024)]
        self.started_at = clock()
        self.archive_error = None
        self.last_error = None
        with self.db() as db:
            db.execute("CREATE TABLE IF NOT EXISTS signals (id TEXT PRIMARY KEY, state TEXT, deadline REAL, data TEXT)")
            db.execute("CREATE TABLE IF NOT EXISTS audit (id TEXT PRIMARY KEY, created REAL, data TEXT, exported INTEGER DEFAULT 0)")

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=2)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    @contextmanager
    def serialized(self, event_id, blocking=True):
        # Bounded, process-stable shards avoid one slow delivery blocking all signals.
        shard = int.from_bytes(hashlib.sha256(event_id.encode()).digest()[:2], "big") % len(self.locks)
        lock = self.locks[shard]
        if not lock.acquire(blocking=blocking):
            yield False
            return
        try:
            with open(f"{self.path}.lock.{shard:03x}", "a") as lockfile:
                try:
                    fcntl.flock(lockfile, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
                except BlockingIOError:
                    yield False
                    return
                try:
                    yield True
                finally:
                    fcntl.flock(lockfile, fcntl.LOCK_UN)
        finally:
            lock.release()

    def deliver(self, data, message, deadline_ts=None):
        timing = data.setdefault("server_timing", {})
        started = self.clock()
        timing["send_started_at_ms"] = round(started * 1000)
        timing["emission_to_send_seconds"] = round(started - data["emitted_ms"] / 1000, 3)
        try:
            return self.send(message, deadline_ts=deadline_ts)
        finally:
            finished = self.clock()
            timing["send_finished_at_ms"] = round(finished * 1000)
            timing["discord_request_seconds"] = round(finished - started, 3)

    def load(self, event_id):
        with self.db() as db:
            row = db.execute("SELECT * FROM signals WHERE id=?", (event_id,)).fetchone()
        if row:
            return dict(row, data=json.loads(row["data"]))
        return None

    def save(self, data, state, deadline=0):
        with self.db() as db:
            db.execute("INSERT OR REPLACE INTO signals VALUES (?, ?, ?, ?)",
                       (data["event_id"], state, deadline, json.dumps(data)))

    def record(self, data, status, **extra):
        audit = dict(data, audit_id=uuid.uuid4().hex, status=status,
                     recorded_at=stamp(self.clock()), actual_result="UNAVAILABLE", **extra)
        with self.db() as db:
            db.execute("INSERT INTO audit(id, created, data) VALUES (?, ?, ?)",
                       (audit["audit_id"], self.clock(), json.dumps(audit)))

    def message(self, data, title, body):
        is_test = data.get("test") is True or str(data.get("ticker", "")).startswith("TEST:")
        prefix = "【テスト・注文禁止】" if is_test else ""
        pair = "USD/JPY" if data["pair"] == "USDJPY" else "BTC/USD"
        direction = "上" if data["signal"] == "HIGH" else "下"
        price = f"{data['signal_price']:.3f}" if data["pair"] == "USDJPY" else f"{data['signal_price']:.2f}"
        time_label = "エントリー予定" if title == "エントリー予告" else "エントリー基準"
        received = data.get("server_timing", {}).get("received_at_ms")
        timing_text = f"発信時刻: {stamp(data['emitted_ms'] / 1000)}\n"
        if received is not None:
            timing_text += f"受信時刻: {stamp(received / 1000)}\n"
        return (f"{prefix}【{title}】{pair} {data['signal']}（{direction}）/ 5分\n"
                f"{body}\n"
                f"{time_label}: {stamp(data['entry_time_ms'] / 1000)}\n"
                f"参考価格: {price} ({data['ticker']})\n\n"
                f"通知時刻: {stamp(self.clock())}（サーバー作成）\n"
                f"{timing_text}"
                f"ID: {data['event_id']}\n"
                "自動発注は行いません。参考判定は実約定・実勝敗とは異なります。")

    def cancel(self, data, reason):
        previous = self.load(data["event_id"])
        if previous and previous["state"] in ("CANCELLED", "CANCEL_PENDING", "ENTERED", "RESULT", "UNKNOWN_RESULT"):
            return {"status": "skipped", "reason": "terminal_state"}
        # Persist the tombstone before transport so delayed entries cannot revive it.
        data = dict(data, cancel_reason=reason)
        self.save(data, "CANCEL_PENDING", self.clock())
        return self.deliver_cancel(data)

    def deliver_cancel(self, data):
        reason = data["cancel_reason"]
        sent = self.deliver(data, self.message(data, "エントリー中止", f"エントリーしないでください。\n理由: {reason}"))
        self.save(data, "CANCELLED" if sent else "CANCEL_PENDING", 0 if sent else self.clock() + 30)
        self.record(data, "CANCELLED", notification_sent=sent, reason=reason)
        return {"status": "cancelled", "notification_sent": sent, "reason": reason}

    def handle(self, raw, allowed=True, received_at=None):
        started = self.clock()
        received_at = started if received_at is None else received_at
        try:
            data = validate(raw)
            if not math.isfinite(received_at) or received_at > started + 1:
                raise ValueError("invalid_server_received_at")
        except (ValueError, KeyError, TypeError, OverflowError) as error:
            return {"status": "rejected", "reason": str(error)}
        with self.serialized(data["event_id"]):
            now = self.clock()
            notice = data["notice"]
            state = self.load(data["event_id"])
            emitted = data["emitted_ms"] / 1000
            entry = data["entry_time_ms"] / 1000
            preview = data["preview_started_ms"] / 1000
            # Ignore client-supplied timing fields; only the trusted route timestamp is used.
            data["server_timing"] = {
                "received_at_ms": round(received_at * 1000),
                "worker_started_at_ms": round(started * 1000),
                "processing_started_at_ms": round(now * 1000),
                "upstream_seconds": round(received_at - emitted, 3),
                "worker_queue_seconds": round(max(0, started - received_at), 3),
                "state_lock_wait_seconds": round(max(0, now - started), 3),
            }
            if emitted > now + 1 or now - emitted > 86400:
                return {"status": "rejected", "reason": "invalid_event_clock"}
            if notice == "PRE_ENTRY_CANCEL":
                # Existing previews must still close outside session hours.
                if not allowed and not state:
                    return {"status": "blocked", "reason": "session_without_preview"}
                return self.cancel(data, "予告後に確定条件が成立しない、または60秒の期限切れ")
            if notice == "REFERENCE_RESULT":
                return self.result(data, state)
            if not allowed:
                if state and state["state"] == "PENDING":
                    return self.cancel(data, "営業時間外")
                return {"status": "blocked", "reason": "session"}
            if state and state["state"] != "PENDING":
                return {"status": "skipped", "reason": "terminal_state"}
            if notice == "PRE_ENTRY":
                if state:
                    return {"status": "skipped", "reason": "duplicate_preview"}
                if preview <= 0 or abs(preview - emitted) > 1 or not (emitted <= entry <= emitted + 60):
                    return {"status": "rejected", "reason": "invalid_preview_clock"}
                deadline = preview + PREVIEW_SECONDS
                if now >= deadline or now - emitted > MAX_ENTRY_AGE:
                    return self.cancel(data, "予告が遅れて到着したため中止")
                self.save(data, "PENDING", deadline)
                sent = self.deliver(data, self.message(data, "エントリー予告",
                                     "待機。確定通知が届くまではエントリーしないでください。\n"
                                     f"確定待ち期限: {stamp(deadline)}\n"
                                     "予告は必ず60秒前ではありません。未確定のままなら予告開始60秒で中止。"),
                                 deadline_ts=min(emitted + MAX_ENTRY_AGE, deadline))
                self.record(data, "PRE_ENTRY", notification_sent=sent)
                if not sent:
                    return self.cancel(data, "予告通知の配信を確認できないため中止")
                return {"status": "pending", "notification_sent": sent}
            deadline = min(entry + MAX_ENTRY_AGE, emitted + MAX_ENTRY_AGE)
            if preview:
                deadline = min(deadline, preview + PREVIEW_SECONDS)
            if state:
                deadline = min(deadline, state["deadline"])
            if (entry < self.started_at or emitted < entry or now < entry
                    or (preview and preview < self.started_at and not state)):
                return self.cancel(data, "再起動前の予告、または時刻が不整合のため中止")
            if now >= deadline:
                return self.cancel(data, "5秒の配信期限、または予告60秒の期限を超過")
            # SENDING is fail-closed if the worker dies during an ambiguous HTTP request.
            self.save(data, "SENDING", deadline)
            sent = self.deliver(data, self.message(data, "確定エントリー",
                            "期限内のみ手動エントリー。期限切れは見送り。\n"
                            f"通知有効期限: {stamp(deadline)}（この時刻以後は入らない）\n"
                            f"5分判定基準: {stamp(entry + EXPIRY_SECONDS)}\n"
                            "業者の受付価格・判定時刻を必ず確認してください。"), deadline_ts=deadline)
            if not sent:
                return self.cancel(data, "期限内の配信を確認できませんでした。エントリーしないでください")
            self.save(data, "ENTERED", entry + EXPIRY_SECONDS + RESULT_GRACE_SECONDS)
            self.record(data, "ENTRY", notification_sent=True, delay_seconds=round(now - entry, 3))
            return {"status": "entered", "notification_sent": True}

    def result(self, data, state):
        if not state or state["state"] != "ENTERED":
            return {"status": "skipped", "reason": "no_accepted_entry"}
        original = state["data"]
        expected = original["entry_time_ms"] + EXPIRY_SECONDS * 1000
        if (data["result_time_ms"] != expected or data["emitted_ms"] < expected
                or data["signal_price"] != original["signal_price"]):
            return {"status": "rejected", "reason": "result_clock_or_price_mismatch"}
        delta = data["result_price"] - original["signal_price"]
        outcome = "DRAW" if delta == 0 else "WIN" if (delta > 0) == (data["signal"] == "HIGH") else "LOSE"
        self.save(data, "RESULT")
        sent = self.deliver(data, self.message(data, "5分参考判定", f"参考結果: {outcome}\n判定参考価格: {data['result_price']}\n"
                                     f"判定基準時刻: {stamp(expected / 1000)}"))
        self.record(data, "REFERENCE_RESULT", reference_result=outcome, notification_sent=sent)
        return {"status": "result", "reference_result": outcome, "notification_sent": sent}

    def sweep(self):
        active_states = ("PENDING", "SENDING", "ENTERED", "CANCEL_PENDING")
        with self.db() as db:
            rows = db.execute("SELECT id FROM signals WHERE state IN ('PENDING', 'SENDING', 'ENTERED', 'CANCEL_PENDING') AND deadline<=?",
                              (self.clock(),)).fetchall()
        for item in rows:
            with self.serialized(item["id"], blocking=False) as acquired:
                if not acquired:
                    continue
                row = self.load(item["id"])
                if not row or row["state"] not in active_states or row["deadline"] > self.clock():
                    continue
                data = row["data"]
                if row["state"] == "CANCEL_PENDING":
                    self.deliver_cancel(data)
                elif row["state"] in ("PENDING", "SENDING"):
                    self.cancel(data, "予告から60秒以内に確定通知が成立しませんでした")
                else:
                    self.save(data, "UNKNOWN_RESULT")
                    sent = self.deliver(data, self.message(data, "参考判定未取得", "5分後の同一配信元の価格を確認できません。勝敗集計から除外します。"))
                    self.record(data, "UNKNOWN_RESULT", notification_sent=sent)

    def export_once(self):
        if not self.archive:
            return
        with self.db() as db:
            rows = db.execute("SELECT * FROM audit WHERE exported=0 ORDER BY created LIMIT 50").fetchall()
        if not rows:
            return
        decoded = [json.loads(row["data"]) for row in rows]
        events = [data for data in decoded if not data.get("test")
                  and not str(data.get("ticker", "")).startswith("TEST:")]
        if events:
            self.archive(events)
        with self.db() as db:
            db.executemany("UPDATE audit SET exported=1 WHERE id=?", [(row["id"],) for row in rows])
        self.archive_error = None

    def start(self):
        def watch():
            while True:
                try:
                    self.sweep()
                    self.last_error = None
                except Exception as error:
                    self.last_error = type(error).__name__
                time.sleep(0.25)

        def export():
            while True:
                try:
                    self.export_once()
                except Exception as error:
                    self.archive_error = type(error).__name__
                time.sleep(30)

        for target in (watch, export):
            threading.Thread(target=target, daemon=True).start()

    def status(self):
        with self.db() as db:
            counts = dict(db.execute("SELECT state, COUNT(*) FROM signals GROUP BY state").fetchall())
            backlog = db.execute("SELECT COUNT(*) FROM audit WHERE exported=0").fetchone()[0]
            last = db.execute("SELECT data FROM audit ORDER BY created DESC LIMIT 1").fetchone()
        return {"schema_version": 2, "mode": "reference_validation", "max_entry_age_seconds": MAX_ENTRY_AGE,
                "pre_entry_cancel_seconds": PREVIEW_SECONDS, "expiry_seconds": EXPIRY_SECONDS,
                "states": counts, "archive_backlog": backlog, "archive_error": self.archive_error,
                "worker_error": self.last_error, "actual_results": "unavailable",
                "serialization": "event_shards_1024", "latency_instrumentation": True,
                "message_format": "live_manual_v1", "notification_time_basis": "server_message_created",
                "last_server_timing": json.loads(last["data"]).get("server_timing") if last else None,
                "state_storage": "local SQLite; not durable across Render redeploys"}
