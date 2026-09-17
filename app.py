from flask import Flask, request
import requests
import os
import threading
import json
import time
import gspread
from collections import defaultdict
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from signal_protocol import ReferenceSignals

app = Flask(__name__)

APP_VERSION = "discord v22 confirmed reference validation"

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

JST = ZoneInfo("Asia/Tokyo")


def notification_time_text(now=None):
    current = now or datetime.now(JST)
    if current.tzinfo is None:
        current = current.replace(tzinfo=JST)
    return current.astimezone(JST).strftime("%Y/%m/%d %H:%M:%S JST")


def append_notification_time(message):
    text = str(message or "").rstrip()
    if "通知時刻:" in text:
        return text
    if not text:
        return f"通知時刻: {notification_time_text()}"
    return f"{text}\n通知時刻: {notification_time_text()}"


def env_bool(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() not in ("0", "false", "no", "off")


def env_int(name, default, minimum=None):
    try:
        value = int(str(os.getenv(name, default)).strip())
    except (TypeError, ValueError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    return value

JUDGE_DELAY_SECONDS = 300
HISTORY_SHEET_NAME = "履歴"
SUMMARY_SHEET_NAME = "日別集計"
LINE_TARGETS_SHEET_NAME = "LINE通知先"
DUPLICATE_WINDOW_SECONDS = 120
LOSS_GUARD_ENABLED = False
LOSS_GUARD_DAILY_LOSSES = env_int("LOSS_GUARD_DAILY_LOSSES", 2, minimum=1)
LOSS_GUARD_LOOKBACK_ENTRIES = env_int("LOSS_GUARD_LOOKBACK_ENTRIES", 5, minimum=1)
LOSS_GUARD_LOOKBACK_LOSSES = env_int("LOSS_GUARD_LOOKBACK_LOSSES", 2, minimum=1)
LOSS_GUARD_COOLDOWN_HOURS = env_int("LOSS_GUARD_COOLDOWN_HOURS", 6, minimum=1)
GOOGLE_API_RETRY_ATTEMPTS = env_int("GOOGLE_API_RETRY_ATTEMPTS", 3, minimum=1)
GOOGLE_API_RETRY_BASE_SECONDS = env_int("GOOGLE_API_RETRY_BASE_SECONDS", 2, minimum=1)
DISCORD_WEBHOOK_URL = (
    os.getenv("DISCORD_WEBHOOK_URL")
    or os.getenv("DISCORD_BO_WEBHOOK_URL")
    or ""
).strip()
DISCORD_USERNAME = os.getenv("DISCORD_USERNAME", "BO Signal Bot").strip()
DISCORD_AVATAR_URL = os.getenv("DISCORD_AVATAR_URL", "").strip()
DISCORD_API_RETRY_ATTEMPTS = env_int("DISCORD_API_RETRY_ATTEMPTS", 3, minimum=1)
DISCORD_API_RETRY_BASE_SECONDS = env_int("DISCORD_API_RETRY_BASE_SECONDS", 2, minimum=1)
DISCORD_API_RETRY_STATUSES = (429, 500, 502, 503, 504)
DISCORD_CONTENT_LIMIT = 1900
PRE_ENTRY_AUTO_CANCEL_ENABLED = env_bool("PRE_ENTRY_AUTO_CANCEL_ENABLED", True)
PRE_ENTRY_AUTO_CANCEL_SECONDS = 60
LINE_API_RETRY_ATTEMPTS = env_int("LINE_API_RETRY_ATTEMPTS", 3, minimum=1)
LINE_API_RETRY_BASE_SECONDS = env_int("LINE_API_RETRY_BASE_SECONDS", 2, minimum=1)
LINE_API_RETRY_STATUSES = (429, 500, 502, 503, 504)
LINE_AUTO_TARGET_ENABLED = env_bool("LINE_AUTO_TARGET_ENABLED", True)
LINE_AUTO_TARGET_LIMIT = env_int("LINE_AUTO_TARGET_LIMIT", 1, minimum=1)
LINE_TARGET_CACHE_SECONDS = env_int("LINE_TARGET_CACHE_SECONDS", 300, minimum=30)
LINE_BROADCAST_FALLBACK_ENABLED = env_bool("LINE_BROADCAST_FALLBACK_ENABLED", False)
LINE_QUOTA_GUARD_ENABLED = env_bool("LINE_QUOTA_GUARD_ENABLED", True)
LINE_QUOTA_CACHE_SECONDS = env_int("LINE_QUOTA_CACHE_SECONDS", 300, minimum=30)
LINE_AUTO_CONFIGURE_WEBHOOK = env_bool("LINE_AUTO_CONFIGURE_WEBHOOK", True)
LINE_WEBHOOK_ENDPOINT_URL = os.getenv(
    "LINE_WEBHOOK_ENDPOINT_URL",
    "https://bo-signal-line.onrender.com/webhook"
).strip()
LINE_WEBHOOK_CONFIG_CACHE_SECONDS = env_int("LINE_WEBHOOK_CONFIG_CACHE_SECONDS", 3600, minimum=60)
THEOPTION_HOURS_FILTER_ENABLED = os.getenv(
    "THEOPTION_HOURS_FILTER_ENABLED",
    "true"
).strip().lower() not in ("0", "false", "no", "off")
THEOPTION_START_HOUR = 8
THEOPTION_END_HOUR = 6

PAIR_SETTINGS = {
    "USDJPY": {
        "display": "USDJPY",
        "symbol": "USD/JPY",
        "price_decimals": 3,
        "start_hour": 8,
        "end_hour": 6,
        "weekdays_only": True,
        "weekends_only": False,
        "session_name": "theoption USDJPY"
    },
    "BTCUSD": {
        "display": "BTC/USD",
        "symbol": "BTC/USD",
        "price_decimals": 2,
        "start_hour": 8,
        "end_hour": 5,
        "weekdays_only": False,
        "weekends_only": True,
        "session_name": "theoption BTC/USD"
    }
}

PAIR_ALIASES = {
    "USDJPY": "USDJPY",
    "USDJPYFX": "USDJPY",
    "BTCUSD": "BTCUSD",
    "BTCUSDT": "BTCUSD"
}

HISTORY_HEADERS = [
    "日時",
    "通貨",
    "足種",
    "方向",
    "エントリー時刻",
    "エントリー価格",
    "判定時刻",
    "判定終了価格",
    "結果"
]

SUMMARY_HEADERS = [
    "日付",
    "総シグナル数",
    "エントリー数",
    "WIN",
    "LOSE",
    "DRAW",
    "CANCEL",
    "勝率"
]

LINE_TARGET_HEADERS = [
    "登録日時",
    "種別",
    "ID",
    "表示名",
    "最終イベント"
]

recent_signal_keys = {}
recent_signal_lock = threading.Lock()
pending_pre_entries = {}
pending_pre_entries_lock = threading.Lock()
last_line_delivery_result = {}
last_line_delivery_lock = threading.Lock()
line_target_cache = {
    "expires_at": 0,
    "targets": [],
    "source": "none",
    "error": None
}
line_target_cache_lock = threading.Lock()
line_quota_cache = {
    "expires_at": 0,
    "status": None
}
line_quota_cache_lock = threading.Lock()
line_webhook_config_cache = {
    "expires_at": 0,
    "status": None
}
line_webhook_config_lock = threading.Lock()


def log(message, *values):
    if values:
        print(message, *values, flush=True)
    else:
        print(message, flush=True)


def log_error(stage, error):
    log(f"ERROR [{stage}]: {type(error).__name__}: {error}")


def google_api_retry_config_status():
    return {
        "attempts": GOOGLE_API_RETRY_ATTEMPTS,
        "base_seconds": GOOGLE_API_RETRY_BASE_SECONDS,
        "retry_statuses": [429, 500, 502, 503, 504]
    }


def line_api_retry_config_status():
    return {
        "attempts": LINE_API_RETRY_ATTEMPTS,
        "base_seconds": LINE_API_RETRY_BASE_SECONDS,
        "retry_statuses": list(LINE_API_RETRY_STATUSES)
    }


def discord_api_retry_config_status():
    return {
        "attempts": DISCORD_API_RETRY_ATTEMPTS,
        "base_seconds": DISCORD_API_RETRY_BASE_SECONDS,
        "retry_statuses": list(DISCORD_API_RETRY_STATUSES)
    }


def discord_config_status():
    return {
        "enabled": True,
        "discord_webhook": bool(DISCORD_WEBHOOK_URL),
        "delivery_mode": "discord_webhook" if DISCORD_WEBHOOK_URL else "not_configured",
        "username": DISCORD_USERNAME or "default"
    }


def discord_delivery_warnings():
    warnings = []
    if not DISCORD_WEBHOOK_URL:
        warnings.append("DISCORD_WEBHOOK_URL is not set")
    return warnings


def google_api_error_status(error):
    response = getattr(error, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code is not None:
        try:
            return int(status_code)
        except (TypeError, ValueError):
            pass

    text = str(error)
    for status in (429, 500, 502, 503, 504):
        if f"[{status}]" in text or f" {status}" in text or f": {status}" in text:
            return status
    return None


def is_transient_google_api_error(error):
    return isinstance(error, APIError) and google_api_error_status(error) in (429, 500, 502, 503, 504)


def retry_google_operation(label, operation):
    for attempt in range(1, GOOGLE_API_RETRY_ATTEMPTS + 1):
        try:
            return operation()
        except Exception as e:
            if not is_transient_google_api_error(e) or attempt >= GOOGLE_API_RETRY_ATTEMPTS:
                raise

            wait_seconds = GOOGLE_API_RETRY_BASE_SECONDS * attempt
            log(
                f"GOOGLE SHEETS RETRY [{label}]:",
                f"attempt={attempt}",
                f"status={google_api_error_status(e)}",
                f"wait={wait_seconds}s"
            )
            time.sleep(wait_seconds)


log("APP VERSION:", APP_VERSION)


def normalize_pair(pair):
    text = str(pair or "USDJPY").strip().upper()
    compact = (
        text.replace("/", "")
        .replace("-", "")
        .replace("_", "")
        .replace(" ", "")
    )
    return PAIR_ALIASES.get(compact, compact)


def get_pair_settings(pair):
    return PAIR_SETTINGS.get(normalize_pair(pair), PAIR_SETTINGS["USDJPY"])


def display_pair(pair):
    return get_pair_settings(pair)["display"]


def get_line_access_token():
    return (
        os.getenv("LINE_ACCESS_TOKEN")
        or os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
        or os.getenv("LINE_BOT_CHANNEL_ACCESS_TOKEN")
        or os.getenv("LINE_BOT_TOKEN")
        or os.getenv("LINE_CHANNEL_TOKEN")
    )


def get_configured_line_targets():
    line_to = (
        os.getenv("LINE_TO")
        or os.getenv("LINE_USER_ID")
        or os.getenv("LINE_GROUP_ID")
        or os.getenv("LINE_USER_IDS")
        or os.getenv("LINE_GROUP_IDS")
    )
    if not line_to:
        return []
    return [target.strip() for target in line_to.split(",") if target.strip()]


def get_line_targets():
    return get_line_targets_with_source()[0]


def get_line_targets_with_source():
    configured_targets = get_configured_line_targets()
    if configured_targets:
        return configured_targets, "env"

    if not LINE_AUTO_TARGET_ENABLED:
        return [], "none"

    now_ts = time.time()
    with line_target_cache_lock:
        if line_target_cache["expires_at"] > now_ts:
            return list(line_target_cache["targets"]), line_target_cache["source"]

    targets, source, error = load_auto_line_targets()
    limited_targets = targets[:LINE_AUTO_TARGET_LIMIT]

    with line_target_cache_lock:
        line_target_cache.update({
            "expires_at": now_ts + LINE_TARGET_CACHE_SECONDS,
            "targets": limited_targets,
            "source": source,
            "error": error
        })

    return limited_targets, source


def get_line_target_cache_status():
    with line_target_cache_lock:
        return {
            "source": line_target_cache["source"],
            "error": line_target_cache["error"],
            "cache_expires_at": line_target_cache["expires_at"]
        }


def line_config_status():
    token_exists = bool(get_line_access_token())
    targets, target_source = get_line_targets_with_source()
    has_targets = bool(targets)
    if len(targets) == 1:
        delivery_mode = "push"
    elif len(targets) > 1:
        delivery_mode = "multicast"
    elif token_exists and LINE_BROADCAST_FALLBACK_ENABLED:
        delivery_mode = "broadcast"
    elif token_exists:
        delivery_mode = "no_target"
    else:
        delivery_mode = "not_configured"

    return {
        "line_token": token_exists,
        "line_to": has_targets,
        "line_target_count": len(targets),
        "line_target_source": target_source,
        "auto_target_enabled": LINE_AUTO_TARGET_ENABLED,
        "broadcast_fallback_enabled": LINE_BROADCAST_FALLBACK_ENABLED,
        "quota_guard_enabled": LINE_QUOTA_GUARD_ENABLED,
        "delivery_mode": delivery_mode
    }


def line_delivery_warnings():
    config = line_config_status()
    warnings = []
    if config["delivery_mode"] == "broadcast":
        warnings.append("LINE_TO is not set; sending by broadcast fallback")
    if config["delivery_mode"] == "no_target":
        warnings.append("LINE target is not set; broadcast fallback is disabled")
    if not config["line_token"]:
        warnings.append("LINE access token is not set")
    quota_status = get_line_quota_status()
    if quota_status.get("exhausted"):
        warnings.append("LINE monthly message quota is exhausted")
    webhook_config = get_line_webhook_config_status()
    if webhook_config.get("available"):
        if webhook_config.get("endpoint") != LINE_WEBHOOK_ENDPOINT_URL:
            warnings.append("LINE webhook endpoint is not configured for this app")
        if webhook_config.get("active") is False:
            warnings.append("LINE webhook is not active")
    return warnings


def set_last_line_delivery_result(result):
    with last_line_delivery_lock:
        last_line_delivery_result.clear()
        last_line_delivery_result.update(result)


def get_last_line_delivery_result():
    with last_line_delivery_lock:
        return dict(last_line_delivery_result)


def get_line_quota_status(force=False):
    if not LINE_QUOTA_GUARD_ENABLED and not force:
        return {"enabled": False}

    token = get_line_access_token()
    if not token:
        return {
            "enabled": LINE_QUOTA_GUARD_ENABLED,
            "available": False,
            "reason": "LINE access token is not set"
        }

    now_ts = time.time()
    with line_quota_cache_lock:
        cached_status = line_quota_cache["status"]
        if not force and cached_status is not None and line_quota_cache["expires_at"] > now_ts:
            return dict(cached_status)

    headers = {"Authorization": f"Bearer {token}"}
    status = {
        "enabled": LINE_QUOTA_GUARD_ENABLED,
        "available": False,
        "exhausted": False,
        "quota": None,
        "consumption": None,
        "reason": ""
    }

    try:
        quota_response = requests.get(
            "https://api.line.me/v2/bot/message/quota",
            headers=headers,
            timeout=10
        )
        consumption_response = requests.get(
            "https://api.line.me/v2/bot/message/quota/consumption",
            headers=headers,
            timeout=10
        )
        status["quota_status_code"] = quota_response.status_code
        status["consumption_status_code"] = consumption_response.status_code

        if quota_response.status_code >= 400 or consumption_response.status_code >= 400:
            status["reason"] = (
                f"quota={quota_response.status_code} "
                f"consumption={consumption_response.status_code}"
            )
        else:
            quota_data = quota_response.json()
            consumption_data = consumption_response.json()
            quota_type = quota_data.get("type")
            quota_value = quota_data.get("value")
            total_usage = (
                consumption_data.get("totalUsage")
                if "totalUsage" in consumption_data
                else consumption_data.get("total_usage")
            )

            status.update({
                "available": True,
                "quota": {
                    "type": quota_type,
                    "value": quota_value
                },
                "consumption": {
                    "total_usage": total_usage
                }
            })

            if quota_type == "limited" and quota_value is not None and total_usage is not None:
                status["remaining"] = max(0, int(quota_value) - int(total_usage))
                status["exhausted"] = int(total_usage) >= int(quota_value)

    except Exception as e:
        status["reason"] = f"{type(e).__name__}: {e}"

    with line_quota_cache_lock:
        line_quota_cache.update({
            "expires_at": now_ts + LINE_QUOTA_CACHE_SECONDS,
            "status": status
        })

    return dict(status)


def get_line_webhook_config_status(force=False):
    token = get_line_access_token()
    if not token:
        return {
            "enabled": LINE_AUTO_CONFIGURE_WEBHOOK,
            "available": False,
            "reason": "LINE access token is not set"
        }

    now_ts = time.time()
    with line_webhook_config_lock:
        cached_status = line_webhook_config_cache["status"]
        if not force and cached_status is not None and line_webhook_config_cache["expires_at"] > now_ts:
            return dict(cached_status)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    status = {
        "enabled": LINE_AUTO_CONFIGURE_WEBHOOK,
        "available": False,
        "desired_endpoint": LINE_WEBHOOK_ENDPOINT_URL,
        "endpoint": None,
        "active": None,
        "updated": False,
        "reason": ""
    }

    try:
        get_response = requests.get(
            "https://api.line.me/v2/bot/channel/webhook/endpoint",
            headers=headers,
            timeout=10
        )
        status["get_status_code"] = get_response.status_code

        if get_response.status_code >= 400:
            status["reason"] = f"get webhook endpoint {get_response.status_code}: {get_response.text[:300]}"
        else:
            data = get_response.json()
            current_endpoint = str(data.get("endpoint", "") or "").strip()
            status.update({
                "available": True,
                "endpoint": current_endpoint or None,
                "active": data.get("active")
            })

            if (
                LINE_AUTO_CONFIGURE_WEBHOOK
                and LINE_WEBHOOK_ENDPOINT_URL
                and current_endpoint != LINE_WEBHOOK_ENDPOINT_URL
            ):
                put_response = requests.put(
                    "https://api.line.me/v2/bot/channel/webhook/endpoint",
                    headers=headers,
                    json={"endpoint": LINE_WEBHOOK_ENDPOINT_URL},
                    timeout=10
                )
                status["put_status_code"] = put_response.status_code
                if put_response.status_code >= 400:
                    status["reason"] = (
                        f"set webhook endpoint {put_response.status_code}: "
                        f"{put_response.text[:300]}"
                    )
                else:
                    status.update({
                        "endpoint": LINE_WEBHOOK_ENDPOINT_URL,
                        "updated": True,
                        "reason": "webhook endpoint updated"
                    })

    except Exception as e:
        status["reason"] = f"{type(e).__name__}: {e}"

    with line_webhook_config_lock:
        line_webhook_config_cache.update({
            "expires_at": now_ts + LINE_WEBHOOK_CONFIG_CACHE_SECONDS,
            "status": status
        })

    return dict(status)


def get_theoption_session_window(current_dt, start_hour=THEOPTION_START_HOUR, end_hour=THEOPTION_END_HOUR):
    if current_dt.tzinfo is None:
        current_dt = current_dt.replace(tzinfo=JST)

    if current_dt.hour >= start_hour:
        session_day = current_dt.date()
    elif current_dt.hour < end_hour:
        session_day = (current_dt - timedelta(days=1)).date()
    else:
        return None

    session_start = datetime(
        session_day.year,
        session_day.month,
        session_day.day,
        start_hour,
        0,
        0,
        tzinfo=JST
    )
    session_end = session_start + timedelta(days=1)
    session_end = session_end.replace(hour=end_hour, minute=0, second=0, microsecond=0)
    return session_start, session_end


def theoption_hours_status(pair="USDJPY", received_at=None, judge_delay_seconds=JUDGE_DELAY_SECONDS):
    if isinstance(pair, datetime):
        received_at = pair
        pair = "USDJPY"

    pair_key = normalize_pair(pair)
    settings = get_pair_settings(pair_key)
    received_at = received_at or datetime.now(JST)
    if received_at.tzinfo is None:
        received_at = received_at.replace(tzinfo=JST)
    judge_at = received_at + timedelta(seconds=judge_delay_seconds)
    session_window = get_theoption_session_window(
        received_at,
        start_hour=settings["start_hour"],
        end_hour=settings["end_hour"]
    )

    status = {
        "enabled": THEOPTION_HOURS_FILTER_ENABLED,
        "pair": settings["display"],
        "allowed": False,
        "now": received_at.strftime("%Y/%m/%d %H:%M:%S"),
        "judge_time": judge_at.strftime("%Y/%m/%d %H:%M:%S"),
        "session_start": None,
        "session_end": None,
        "start_hour": settings["start_hour"],
        "end_hour": settings["end_hour"],
        "weekdays_only": settings["weekdays_only"],
        "weekends_only": settings["weekends_only"],
        "reason": ""
    }

    if not THEOPTION_HOURS_FILTER_ENABLED:
        status["allowed"] = True
        status["reason"] = "filter disabled"
        return status

    if session_window is None:
        status["reason"] = f"outside {settings['session_name']} session"
        return status

    session_start, session_end = session_window
    status["session_start"] = session_start.strftime("%Y/%m/%d %H:%M:%S")
    status["session_end"] = session_end.strftime("%Y/%m/%d %H:%M:%S")

    session_is_weekend = session_start.weekday() > 4

    if settings["weekdays_only"] and session_is_weekend:
        status["reason"] = "weekend session"
        return status

    if settings["weekends_only"] and not session_is_weekend:
        status["reason"] = "weekday session"
        return status

    if received_at < session_start or received_at >= session_end:
        status["reason"] = f"outside {settings['session_name']} session"
        return status

    if judge_at >= session_end:
        status["reason"] = "judge time exceeds session close"
        return status

    status["allowed"] = True
    status["reason"] = f"inside {settings['session_name']} session"
    return status


def get_last_notification_delivery_result():
    return get_last_line_delivery_result()


def set_last_notification_delivery_result(result):
    set_last_line_delivery_result(result)


def truncate_discord_content(message):
    text = str(message)
    if len(text) <= DISCORD_CONTENT_LIMIT:
        return text
    return text[:DISCORD_CONTENT_LIMIT - 20] + "\n...(truncated)"


def discord_retry_after_seconds(response, attempt):
    try:
        data = response.json()
        retry_after = data.get("retry_after")
        if retry_after is not None:
            return max(float(retry_after), 1.0)
    except Exception:
        pass
    return DISCORD_API_RETRY_BASE_SECONDS * attempt


def send_discord_message(message, deadline_ts=None):
    delivery_mode = "discord_webhook"
    try:
        if not DISCORD_WEBHOOK_URL:
            set_last_notification_delivery_result({
                "ok": False,
                "channel": "discord",
                "delivery_mode": "not_configured",
                "error": "DISCORD_WEBHOOK_URL is not set"
            })
            log("DISCORD WEBHOOK MISSING")
            return False

        payload = {
            "content": truncate_discord_content(message),
            "allowed_mentions": {"parse": []}
        }
        if DISCORD_USERNAME:
            payload["username"] = DISCORD_USERNAME
        if DISCORD_AVATAR_URL:
            payload["avatar_url"] = DISCORD_AVATAR_URL

        for attempt in range(1, DISCORD_API_RETRY_ATTEMPTS + 1):
            remaining = None if deadline_ts is None else deadline_ts - time.time()
            if remaining is not None and remaining <= 0:
                set_last_notification_delivery_result({"ok": False, "channel": "discord", "error": "delivery_deadline_expired"})
                return False
            response = None
            try:
                timeout = 10 if remaining is None else (min(1, remaining / 2), max(0.001, remaining / 2))
                response = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=timeout)

                log("DISCORD STATUS:", response.status_code, f"attempt={attempt}")
                log("DISCORD RESPONSE:", response.text[:500])

                if response.status_code < 400:
                    timely = deadline_ts is None or time.time() < deadline_ts
                    set_last_notification_delivery_result({
                        "ok": timely,
                        "channel": "discord",
                        "status_code": response.status_code,
                        "attempt": attempt,
                        "delivery_mode": delivery_mode,
                        "response": response.text[:500]
                    })
                    return timely

                error = RuntimeError(f"Discord webhook error {response.status_code}: {response.text}")
                should_retry = response.status_code in DISCORD_API_RETRY_STATUSES
                set_last_notification_delivery_result({
                    "ok": False,
                    "channel": "discord",
                    "status_code": response.status_code,
                    "attempt": attempt,
                    "delivery_mode": delivery_mode,
                    "response": response.text[:500],
                    "will_retry": should_retry
                })
            except requests.RequestException as e:
                error = e
                should_retry = True
                set_last_notification_delivery_result({
                    "ok": False,
                    "channel": "discord",
                    "attempt": attempt,
                    "delivery_mode": delivery_mode,
                    "error": f"{type(e).__name__}: {e}",
                    "will_retry": True
                })

            if not should_retry or attempt >= DISCORD_API_RETRY_ATTEMPTS:
                raise error

            wait_seconds = (
                discord_retry_after_seconds(response, attempt)
                if response is not None
                else DISCORD_API_RETRY_BASE_SECONDS * attempt
            )
            if deadline_ts is not None and time.time() + wait_seconds >= deadline_ts:
                return False
            log(
                "DISCORD RETRY:",
                f"attempt={attempt}",
                f"wait={wait_seconds}s",
                f"error={error}"
            )
            time.sleep(wait_seconds)

        return False

    except Exception as e:
        current_result = get_last_notification_delivery_result()
        current_result.update({
            "ok": False,
            "channel": "discord",
            "delivery_mode": delivery_mode,
            "error": f"{type(e).__name__}: {e}"
        })
        set_last_notification_delivery_result(current_result)
        log_error("DISCORD SEND", e)
        return False


def send_notification_message(message, deadline_ts=None):
    return send_discord_message(append_notification_time(message), deadline_ts=deadline_ts)


def send_line_message(message):
    return send_notification_message(message)


def notify_error(title, error):
    message = f"⚠️ {title}\n{type(error).__name__}: {error}"
    send_notification_message(message)


def get_price_for_pair(pair):
    try:
        if not TWELVE_DATA_API_KEY:
            raise RuntimeError("TWELVE_DATA_API_KEY is not set")

        settings = get_pair_settings(pair)
        url = "https://api.twelvedata.com/price"
        params = {
            "symbol": settings["symbol"],
            "apikey": TWELVE_DATA_API_KEY
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        log("PRICE RESPONSE:", data)

        if "price" not in data:
            raise RuntimeError(f"価格取得失敗: {data}")

        price = float(data["price"])
        return price

    except Exception as e:
        log_error("TWELVE DATA PRICE", e)
        raise


def get_usdjpy_price():
    return get_price_for_pair("USDJPY")


def get_spreadsheet():
    try:
        if not SPREADSHEET_ID:
            raise RuntimeError("SPREADSHEET_ID is not set")
        if not GOOGLE_SERVICE_ACCOUNT_JSON:
            raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        service_account_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        credentials = Credentials.from_service_account_info(
            service_account_info,
            scopes=scopes
        )
        client = gspread.authorize(credentials)
        return retry_google_operation(
            "OPEN SPREADSHEET",
            lambda: client.open_by_key(SPREADSHEET_ID)
        )

    except Exception as e:
        log_error("GOOGLE SHEETS AUTH", e)
        raise


def update_values(worksheet, range_name, values):
    try:
        def do_update():
            try:
                return worksheet.update(
                    range_name=range_name,
                    values=values,
                    value_input_option="USER_ENTERED"
                )
            except TypeError:
                return worksheet.update(
                    range_name,
                    values,
                    value_input_option="USER_ENTERED"
                )

        retry_google_operation("UPDATE VALUES", do_update)
    except Exception as e:
        log_error("GOOGLE SHEETS UPDATE VALUES", e)
        raise


def get_or_create_worksheet(sheet_name, headers):
    try:
        spreadsheet = get_spreadsheet()

        try:
            worksheet = retry_google_operation(
                f"FIND WORKSHEET {sheet_name}",
                lambda: spreadsheet.worksheet(sheet_name)
            )
        except WorksheetNotFound:
            worksheet = None

            if sheet_name == HISTORY_SHEET_NAME:
                worksheets = retry_google_operation(
                    "LIST WORKSHEETS",
                    spreadsheet.worksheets
                )
                if worksheets and worksheets[0].title != SUMMARY_SHEET_NAME:
                    worksheet = worksheets[0]
                    if worksheet.title != HISTORY_SHEET_NAME:
                        retry_google_operation(
                            f"RENAME WORKSHEET {worksheet.title}",
                            lambda: worksheet.update_title(HISTORY_SHEET_NAME)
                        )

            if worksheet is None:
                worksheet = retry_google_operation(
                    f"ADD WORKSHEET {sheet_name}",
                    lambda: spreadsheet.add_worksheet(
                        title=sheet_name,
                        rows=1000,
                        cols=len(headers)
                    )
                )

        ensure_headers(worksheet, headers)

        return worksheet

    except Exception as e:
        log_error(f"GET OR CREATE WORKSHEET {sheet_name}", e)
        raise


def ensure_headers(worksheet, headers):
    try:
        first_row = retry_google_operation(
            "READ HEADERS",
            lambda: worksheet.row_values(1)
        )
        if first_row[:len(headers)] == headers:
            return

        first_row_has_value = any(str(value).strip() for value in first_row)
        first_row_looks_like_header = any(value in headers for value in first_row)

        if first_row_has_value and not first_row_looks_like_header:
            retry_google_operation(
                "INSERT HEADERS",
                lambda: worksheet.insert_row(headers, index=1, value_input_option="USER_ENTERED")
            )
        else:
            update_values(worksheet, f"A1:{chr(64 + len(headers))}1", [headers])

    except Exception as e:
        log_error("ENSURE HEADERS", e)
        raise


def get_history_sheet():
    return get_or_create_worksheet(HISTORY_SHEET_NAME, HISTORY_HEADERS)


def get_line_targets_sheet():
    return get_or_create_worksheet(LINE_TARGETS_SHEET_NAME, LINE_TARGET_HEADERS)


def invalidate_line_target_cache():
    with line_target_cache_lock:
        line_target_cache.update({
            "expires_at": 0,
            "targets": [],
            "source": "none",
            "error": None
        })


def load_saved_line_targets():
    try:
        sheet = get_line_targets_sheet()
        rows = retry_google_operation(
            "READ LINE TARGETS",
            sheet.get_all_values
        )

        seen = set()
        targets = []
        for row in reversed(rows[1:]):
            if len(row) < 3:
                continue
            target_id = str(row[2] or "").strip()
            if not target_id or target_id in seen:
                continue
            seen.add(target_id)
            targets.append(target_id)

        return targets, None

    except Exception as e:
        log_error("LINE TARGETS READ", e)
        return [], f"{type(e).__name__}: {e}"


def fetch_line_follower_targets():
    token = get_line_access_token()
    if not token:
        return [], "LINE access token is not set"

    try:
        response = requests.get(
            "https://api.line.me/v2/bot/followers/ids",
            headers={"Authorization": f"Bearer {token}"},
            params={"limit": 300},
            timeout=10
        )
        if response.status_code >= 400:
            return [], f"LINE followers API {response.status_code}: {response.text[:300]}"

        data = response.json()
        user_ids = [
            str(user_id).strip()
            for user_id in data.get("userIds", [])
            if str(user_id).strip()
        ]
        return user_ids, None

    except Exception as e:
        return [], f"{type(e).__name__}: {e}"


def load_auto_line_targets():
    saved_targets, saved_error = load_saved_line_targets()
    if saved_targets:
        return saved_targets, "sheet", saved_error

    follower_targets, follower_error = fetch_line_follower_targets()
    if follower_targets:
        return follower_targets, "followers_api", follower_error

    return [], "none", saved_error or follower_error


def save_line_target(target_type, target_id, display_name="", event_type=""):
    target_id = str(target_id or "").strip()
    if not target_id:
        return False

    try:
        sheet = get_line_targets_sheet()
        rows = retry_google_operation(
            "READ LINE TARGETS BEFORE SAVE",
            sheet.get_all_values
        )
        existing_ids = {
            str(row[2] or "").strip()
            for row in rows[1:]
            if len(row) >= 3
        }
        if target_id in existing_ids:
            return False

        now = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")
        row = [
            now,
            str(target_type or "").strip(),
            target_id,
            str(display_name or "").strip(),
            str(event_type or "").strip()
        ]
        retry_google_operation(
            "APPEND LINE TARGET",
            lambda: sheet.append_row(row, value_input_option="USER_ENTERED")
        )
        invalidate_line_target_cache()
        return True

    except Exception as e:
        log_error("LINE TARGET SAVE", e)
        return False


def extract_date(value):
    text = str(value or "").strip()
    if not text:
        return None

    for fmt in (
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d",
        "%Y-%m-%d",
        "%Y年%m月%d日 %H:%M:%S",
        "%Y年%m月%d日 %H:%M",
        "%Y年%m月%d日"
    ):
        try:
            return datetime.strptime(text, fmt).strftime("%Y/%m/%d")
        except ValueError:
            pass

    head = text.split()[0].replace("-", "/").replace(".", "/")
    if len(head) >= 10:
        return head[:10]
    return None


def normalize_result(value):
    return str(value or "").strip().upper()


def parse_history_datetime(value):
    text = str(value or "").strip()
    if not text:
        return None

    for fmt in (
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y-%m-%d %H:%M",
        "%Y年%m月%d日 %H:%M:%S",
        "%Y年%m月%d日 %H:%M"
    ):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=JST)
        except ValueError:
            pass

    return None


def risk_guard_config_status():
    return {
        "enabled": LOSS_GUARD_ENABLED,
        "daily_losses": LOSS_GUARD_DAILY_LOSSES,
        "lookback_entries": LOSS_GUARD_LOOKBACK_ENTRIES,
        "lookback_losses": LOSS_GUARD_LOOKBACK_LOSSES,
        "cooldown_hours": LOSS_GUARD_COOLDOWN_HOURS
    }


def get_loss_guard_status(pair, received_at=None):
    pair_display = display_pair(pair)
    now = received_at or datetime.now(JST)
    if now.tzinfo is None:
        now = now.replace(tzinfo=JST)

    status = {
        "enabled": LOSS_GUARD_ENABLED,
        "allowed": True,
        "pair": pair_display,
        "reason": "loss guard disabled",
        "today_losses": 0,
        "daily_loss_limit": LOSS_GUARD_DAILY_LOSSES,
        "recent_entries": 0,
        "recent_losses": 0,
        "lookback_entries": LOSS_GUARD_LOOKBACK_ENTRIES,
        "lookback_loss_limit": LOSS_GUARD_LOOKBACK_LOSSES,
        "cooldown_hours": LOSS_GUARD_COOLDOWN_HOURS,
        "cooldown_until": None
    }

    if not LOSS_GUARD_ENABLED:
        return status

    status["reason"] = "loss guard clear"

    try:
        history_sheet = get_history_sheet()
        rows = retry_google_operation(
            "LOSS GUARD HISTORY READ",
            history_sheet.get_all_values
        )
    except Exception as e:
        log_error("LOSS GUARD HISTORY READ", e)
        status["reason"] = "loss guard unavailable; fail open"
        return status

    today_key = now.strftime("%Y/%m/%d")
    completed = []

    for row in rows[1:]:
        if len(row) < 9:
            continue

        row_pair = display_pair(row[1] if len(row) > 1 else "")
        if row_pair != pair_display:
            continue

        result = normalize_result(row[8])
        if result not in ("WIN", "LOSE", "DRAW"):
            continue

        date_key = extract_date(row[0] if len(row) > 0 else "")
        row_dt = parse_history_datetime(row[0] if len(row) > 0 else "") or parse_history_datetime(row[4] if len(row) > 4 else "")

        completed.append({
            "date": date_key,
            "datetime": row_dt,
            "result": result
        })

        if date_key == today_key and result == "LOSE":
            status["today_losses"] += 1

    if status["today_losses"] >= LOSS_GUARD_DAILY_LOSSES:
        status["allowed"] = False
        status["reason"] = f"同日{LOSS_GUARD_DAILY_LOSSES}敗に到達"
        next_day = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        status["cooldown_until"] = next_day.strftime("%Y/%m/%d %H:%M:%S")
        return status

    recent = completed[-LOSS_GUARD_LOOKBACK_ENTRIES:]
    status["recent_entries"] = len(recent)
    status["recent_losses"] = sum(1 for item in recent if item["result"] == "LOSE")

    if status["recent_losses"] >= LOSS_GUARD_LOOKBACK_LOSSES:
        latest_loss_dt = next(
            (
                item["datetime"]
                for item in reversed(recent)
                if item["result"] == "LOSE" and item["datetime"] is not None
            ),
            None
        )
        if latest_loss_dt is not None:
            cooldown_until = latest_loss_dt + timedelta(hours=LOSS_GUARD_COOLDOWN_HOURS)
            if now < cooldown_until:
                status["allowed"] = False
                status["reason"] = (
                    f"直近{LOSS_GUARD_LOOKBACK_ENTRIES}回中"
                    f"{LOSS_GUARD_LOOKBACK_LOSSES}敗に到達"
                )
                status["cooldown_until"] = cooldown_until.strftime("%Y/%m/%d %H:%M:%S")

    return status


def update_daily_summary():
    try:
        history_sheet = get_history_sheet()
        summary_sheet = get_or_create_worksheet(SUMMARY_SHEET_NAME, SUMMARY_HEADERS)
        rows = retry_google_operation(
            "READ HISTORY FOR DAILY SUMMARY",
            history_sheet.get_all_values
        )

        daily = defaultdict(lambda: {
            "total": 0,
            "WIN": 0,
            "LOSE": 0,
            "DRAW": 0,
            "CANCEL": 0
        })

        for row in rows[1:]:
            if not row:
                continue

            date_key = extract_date(row[0] if len(row) > 0 else "")
            if not date_key:
                continue

            result = normalize_result(row[8] if len(row) > 8 else "")
            daily[date_key]["total"] += 1

            if result in ("WIN", "LOSE", "DRAW", "CANCEL"):
                daily[date_key][result] += 1

        values = [SUMMARY_HEADERS]
        for date_key in sorted(daily.keys()):
            counts = daily[date_key]
            entry_count = counts["WIN"] + counts["LOSE"] + counts["DRAW"]
            win_rate = counts["WIN"] / entry_count if entry_count else 0

            values.append([
                date_key,
                counts["total"],
                entry_count,
                counts["WIN"],
                counts["LOSE"],
                counts["DRAW"],
                counts["CANCEL"],
                win_rate
            ])

        retry_google_operation("CLEAR DAILY SUMMARY", summary_sheet.clear)
        update_values(summary_sheet, f"A1:H{len(values)}", values)

        try:
            retry_google_operation(
                "FORMAT DAILY SUMMARY",
                lambda: summary_sheet.format("H2:H", {
                    "numberFormat": {
                        "type": "PERCENT",
                        "pattern": "0.0%"
                    }
                })
            )
        except Exception as e:
            log_error("DAILY SUMMARY FORMAT", e)

        log("DAILY SUMMARY UPDATED")

    except Exception as e:
        log_error("DAILY SUMMARY UPDATE", e)
        raise


def safe_update_daily_summary():
    try:
        update_daily_summary()
    except Exception:
        pass


def append_entry_row(pair, timeframe, signal, entry_time, entry_price, judge_time):
    try:
        sheet = get_history_sheet()
        now = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")

        row = [
            now,
            pair,
            timeframe,
            signal,
            entry_time,
            entry_price,
            judge_time,
            "",
            ""
        ]

        retry_google_operation(
            "APPEND ENTRY ROW",
            lambda: sheet.append_row(row, value_input_option="USER_ENTERED")
        )
        rows = retry_google_operation("COUNT HISTORY ROWS", sheet.get_all_values)
        row_number = len(rows)

        log("SHEET APPENDED ROW:", row_number)
        safe_update_daily_summary()

        return row_number

    except Exception as e:
        log_error("SHEET APPEND ENTRY ROW", e)
        raise


def format_price(price, pair="USDJPY"):
    decimals = get_pair_settings(pair)["price_decimals"]
    return f"{float(price):.{decimals}f}"


def format_optional_price(price, pair="USDJPY"):
    text = str(price or "").strip()
    if not text or is_tradingview_placeholder(text):
        return "N/A"

    try:
        return format_price(text, pair)
    except (TypeError, ValueError):
        return text


def update_result(row_number, judge_price, result):
    try:
        sheet = get_history_sheet()
        retry_google_operation(
            "UPDATE JUDGE PRICE",
            lambda: sheet.update_cell(row_number, 8, judge_price)
        )
        retry_google_operation(
            "UPDATE JUDGE RESULT",
            lambda: sheet.update_cell(row_number, 9, result)
        )
        log("RESULT UPDATED:", judge_price, result)
        safe_update_daily_summary()
    except Exception as e:
        log_error("SHEET UPDATE RESULT", e)
        raise


def judge_result(signal, entry_price, judge_price):
    if signal == "HIGH":
        if judge_price > entry_price:
            return "WIN"
        if judge_price < entry_price:
            return "LOSE"
        return "DRAW"

    if signal == "LOW":
        if judge_price < entry_price:
            return "WIN"
        if judge_price > entry_price:
            return "LOSE"
        return "DRAW"

    return "UNKNOWN"


def judge_and_update_sheet(signal, pair, timeframe, row_number, entry_price):
    try:
        judge_price = get_price_for_pair(pair)
        judge_price_text = format_price(judge_price, pair)
        log("JUDGE PRICE:", judge_price)

        result = judge_result(signal, entry_price, judge_price)
        sheet_note = ""
        if row_number is None:
            sheet_note = "\n\n記録: エントリー時にGoogle Sheets未記録のため、判定結果も未記録です。"
        else:
            try:
                update_result(row_number, judge_price_text, result)
            except Exception as e:
                log_error("SHEET UPDATE RESULT NONBLOCKING", e)
                sheet_note = "\n\n記録: Google Sheets一時エラーのため、判定結果は未反映です。"

        message = (
            f"📊【判定結果】\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n\n"
            f"エントリー価格: {format_price(entry_price, pair)}\n"
            f"判定終了価格: {judge_price_text}\n"
            f"結果: {result}"
            f"{sheet_note}"
        )

        if send_notification_message(message):
            log("JUDGE NOTICE SENT")

    except Exception as e:
        log_error("JUDGE PROCESS", e)
        notify_error("判定処理エラー", e)


def process_signal(data):
    try:
        log("PROCESS START:", data)

        signal = str(data.get("signal", "UNKNOWN")).strip().upper()
        pair = display_pair(data.get("pair", "USDJPY"))
        timeframe = str(data.get("timeframe", "5")).strip()
        signal_price = str(data.get("signal_price", "")).strip()
        signal_price_text = format_optional_price(signal_price, pair)

        log("SIGNAL:", signal)
        log("PAIR:", pair)
        log("TIMEFRAME:", timeframe)
        log("SIGNAL PRICE:", signal_price_text)

        now = datetime.now(JST)
        judge_dt = now + timedelta(seconds=JUDGE_DELAY_SECONDS)

        entry_time = now.strftime("%Y/%m/%d %H:%M:%S")
        judge_time = judge_dt.strftime("%Y/%m/%d %H:%M:%S")

        entry_price = get_price_for_pair(pair)
        entry_price_text = format_price(entry_price, pair)
        log("ENTRY PRICE:", entry_price)

        row_number = None
        sheet_note = ""
        try:
            row_number = append_entry_row(
                pair=pair,
                timeframe=timeframe,
                signal=signal,
                entry_time=entry_time,
                entry_price=entry_price_text,
                judge_time=judge_time
            )
        except Exception as e:
            log_error("SHEET APPEND ENTRY ROW NONBLOCKING", e)
            sheet_note = "\n\n記録: Google Sheets一時エラーのため未記録です。通知と判定は継続します。"

        message = (
            f"🔴【即エントリー通知】\n\n"
            f"今すぐエントリー\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n"
            f"シグナル時価格: {signal_price_text}\n"
            f"エントリー価格: {entry_price_text}\n"
            f"判定予定時刻: {judge_time}"
            f"{sheet_note}"
        )

        sent = send_notification_message(message)
        if sent:
            log("ENTRY NOTICE SENT")

        timer = threading.Timer(
            JUDGE_DELAY_SECONDS,
            judge_and_update_sheet,
            args=[signal, pair, timeframe, row_number, entry_price]
        )
        timer.daemon = True
        timer.start()
        return sent

    except Exception as e:
        log_error("SIGNAL PROCESS", e)
        notify_error("シグナル処理エラー", e)
        return False


def get_notice_type(data):
    notice = str(data.get("notice", data.get("type", ""))).strip().upper()
    return notice


def is_pre_entry_start_notice(data):
    return get_notice_type(data) in (
        "PRE_ENTRY",
        "PRE",
        "ENTRY_PREVIEW"
    )


def is_pre_entry_terminal_notice(data):
    return get_notice_type(data) in (
        "PRE_ENTRY_CANCEL",
        "PRE_ENTRY_NO_ENTRY",
        "PRE_ENTRY_NO_ENTRY_1MIN",
        "PRE_ENTRY_PENDING"
    )


def is_pre_entry_notice(data):
    return is_pre_entry_start_notice(data) or is_pre_entry_terminal_notice(data)


def build_pre_entry_pending_key(data):
    signal = str(data.get("signal", "UNKNOWN")).strip().upper()
    pair = normalize_pair(data.get("pair", "USDJPY"))
    timeframe = duplicate_key_value(data.get("timeframe", "1"), "1")
    return pair, timeframe, signal


def start_pre_entry_auto_cancel(data, received_at):
    if not PRE_ENTRY_AUTO_CANCEL_ENABLED:
        return None

    key = build_pre_entry_pending_key(data)
    state_id = f"{received_at.timestamp()}:{time.monotonic()}"
    deadline_ts = time.time() + PRE_ENTRY_AUTO_CANCEL_SECONDS
    data_snapshot = dict(data)

    with pending_pre_entries_lock:
        pending_pre_entries[key] = {
            "state_id": state_id,
            "data": data_snapshot,
            "created_at": received_at.strftime("%Y/%m/%d %H:%M:%S"),
            "deadline_ts": deadline_ts
        }

    timer = threading.Timer(
        PRE_ENTRY_AUTO_CANCEL_SECONDS,
        auto_cancel_pre_entry_if_still_pending,
        args=[key, state_id]
    )
    timer.daemon = True
    timer.start()
    log(
        "PRE ENTRY AUTO CANCEL SCHEDULED:",
        key,
        f"seconds={PRE_ENTRY_AUTO_CANCEL_SECONDS}"
    )
    return state_id


def mark_pre_entry_finished(data, status):
    key = build_pre_entry_pending_key(data)
    with pending_pre_entries_lock:
        removed = pending_pre_entries.pop(key, None)
    if removed:
        log("PRE ENTRY AUTO CANCEL CLEARED:", key, f"status={status}")
        return True
    return False


def auto_cancel_pre_entry_if_still_pending(key, state_id):
    with pending_pre_entries_lock:
        state = pending_pre_entries.get(key)
        if not state or state.get("state_id") != state_id:
            return
        pending_pre_entries.pop(key, None)

    data = dict(state.get("data") or {})
    data["notice"] = "PRE_ENTRY_CANCEL"
    data["reason"] = "server_auto_no_entry_after_1min"
    log("PRE ENTRY AUTO CANCEL FIRED:", key, data)
    process_pre_entry_notice(data)


def pre_entry_auto_cancel_status():
    now_ts = time.time()
    with pending_pre_entries_lock:
        pending = []
        for key, state in pending_pre_entries.items():
            pair, timeframe, signal = key
            pending.append({
                "pair": pair,
                "timeframe": timeframe,
                "signal": signal,
                "created_at": state.get("created_at"),
                "seconds_until_cancel": max(0, int(state.get("deadline_ts", now_ts) - now_ts))
            })

    return {
        "enabled": PRE_ENTRY_AUTO_CANCEL_ENABLED,
        "seconds": PRE_ENTRY_AUTO_CANCEL_SECONDS,
        "pending_count": len(pending),
        "pending": pending[:10]
    }


def process_pre_entry_notice(data):
    try:
        log("PRE ENTRY NOTICE START:", data)

        notice = get_notice_type(data)
        signal = str(data.get("signal", "UNKNOWN")).strip().upper()
        pair = display_pair(data.get("pair", "USDJPY"))
        timeframe = str(data.get("timeframe", "1")).strip()
        signal_price = str(data.get("signal_price", "")).strip()
        signal_price_text = format_optional_price(signal_price, pair)
        alert_time = str(data.get("alert_time", "")).strip()
        reason = str(data.get("reason", "")).strip()
        reason_labels = {
            "signal_lost_on_close": "1分足確定時に条件不成立",
            "no_entry_after_1min": "予告から1分後にエントリーなし",
            "no_entry_after_1m_bar_close": "1分足確定時にエントリーなし",
            "no_entry_after_next_bar_fallback": "1分足確定通知を取りこぼしたため次の足で中止",
            "no_entry_after_1m_timeout": "予告から1分経過後にエントリーなし",
            "no_final_status_after_1min": "予告から1分経過後も最終判定なし",
            "server_auto_no_entry_after_1min": "サーバー側で予告から約1分後まで即エントリー未受信"
        }
        reason_text = reason_labels.get(reason, reason)
        now = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")

        if notice in ("PRE_ENTRY_CANCEL", "PRE_ENTRY_NO_ENTRY", "PRE_ENTRY_NO_ENTRY_1MIN", "PRE_ENTRY_PENDING"):
            title = "⚪【エントリー中止】"
            body = (
                "予告から約1分後に確定エントリーが出ませんでした。\n"
                "今回はエントリー見送りです。"
            )
        else:
            title = "🟡【エントリー予告】"
            body = (
                "まだ確定ではありません。\n"
                "このまま1分足が確定するとエントリー通知になる可能性があります。"
            )

        reason_line = f"\n理由: {reason_text}" if reason_text else ""
        message = (
            f"{title}\n\n"
            f"{body}\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n"
            f"現在価格: {signal_price_text}\n"
            f"TradingView時刻: {alert_time or 'N/A'}\n"
            f"受信時刻: {now}"
            f"{reason_line}"
        )

        sent = send_notification_message(message)
        if sent:
            log("PRE ENTRY NOTICE SENT")
        return sent

    except Exception as e:
        log_error("PRE ENTRY NOTICE", e)
        notify_error("エントリー予告処理エラー", e)
        return False


def is_test_payload(data):
    fields = (
        data.get("signal", ""),
        data.get("timeframe", ""),
        data.get("reason", ""),
        data.get("notice", data.get("type", ""))
    )
    text = " ".join(str(value) for value in fields).upper()
    return "TEST" in text or "テスト" in text


def process_test_entry_notice(data):
    try:
        log("TEST ENTRY NOTICE START:", data)

        signal = str(data.get("signal", "UNKNOWN")).strip().upper()
        pair = display_pair(data.get("pair", "USDJPY"))
        timeframe = str(data.get("timeframe", "1")).strip()
        signal_price = str(data.get("signal_price", "")).strip()
        signal_price_text = format_optional_price(signal_price, pair)
        alert_time = str(data.get("alert_time", "")).strip()
        reason = str(data.get("reason", "")).strip()
        now = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")

        reason_line = f"\n理由: {reason}" if reason else ""
        message = (
            "🔴【即エントリー通知 テスト】\n\n"
            "通知経路のテストです。実エントリー・履歴記録・5分後判定は行いません。\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n"
            f"シグナル時価格: {signal_price_text}\n"
            f"TradingView時刻: {alert_time or 'N/A'}\n"
            f"受信時刻: {now}"
            f"{reason_line}"
        )

        sent = send_notification_message(message)
        if sent:
            log("TEST ENTRY NOTICE SENT")
        return sent

    except Exception as e:
        log_error("TEST ENTRY NOTICE", e)
        notify_error("即エントリーテスト処理エラー", e)
        return False


def is_tradingview_placeholder(value):
    text = str(value or "").strip()
    return text.startswith("{{") and text.endswith("}}")


def duplicate_key_value(value, fallback=""):
    text = str(value or "").strip()
    if not text or is_tradingview_placeholder(text):
        return fallback
    return text


def should_apply_loss_guard(data):
    return False


def notify_loss_guard_block(data, guard_status):
    signal = str(data.get("signal", "UNKNOWN")).strip().upper()
    pair = display_pair(data.get("pair", "USDJPY"))
    timeframe = str(data.get("timeframe", "1")).strip()
    signal_price = str(data.get("signal_price", "")).strip()
    signal_price_text = format_optional_price(signal_price, pair)
    notice = get_notice_type(data) or "ENTRY"
    now = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")

    cooldown_until = guard_status.get("cooldown_until") or "N/A"
    message = (
        "🛑【エントリー停止】\n\n"
        "連敗ストップ条件により、今回は通知/エントリーを停止しました。\n\n"
        f"通貨: {pair}\n"
        f"足種: {timeframe}\n"
        f"方向: {signal}\n"
        f"通知種別: {notice}\n"
        f"シグナル時価格: {signal_price_text}\n"
        f"理由: {guard_status.get('reason', 'loss guard')}\n"
        f"本日LOSE数: {guard_status.get('today_losses', 0)} / {guard_status.get('daily_loss_limit', LOSS_GUARD_DAILY_LOSSES)}\n"
        f"直近LOSE数: {guard_status.get('recent_losses', 0)} / {guard_status.get('lookback_loss_limit', LOSS_GUARD_LOOKBACK_LOSSES)}"
        f"（直近{guard_status.get('lookback_entries', LOSS_GUARD_LOOKBACK_ENTRIES)}回）\n"
        f"再開目安: {cooldown_until}\n"
        f"受信時刻: {now}"
    )

    sent = send_notification_message(message)
    if sent:
        log("LOSS GUARD BLOCK NOTICE SENT")
    return sent


def build_duplicate_key(data, received_at):
    received_minute = received_at.strftime("%Y/%m/%d %H:%M")
    signal_time = duplicate_key_value(data.get("alert_time", ""), received_minute)

    return (
        str(data.get("notice", data.get("type", ""))).strip().upper(),
        str(data.get("signal", "")).strip().upper(),
        str(data.get("pair", "")).strip(),
        duplicate_key_value(data.get("timeframe", "")),
        duplicate_key_value(data.get("signal_price", "")),
        signal_time
    )


def is_duplicate_signal(data, received_at):
    key = build_duplicate_key(data, received_at)
    now_ts = received_at.timestamp()

    with recent_signal_lock:
        expired_keys = [
            saved_key
            for saved_key, saved_ts in recent_signal_keys.items()
            if now_ts - saved_ts > DUPLICATE_WINDOW_SECONDS
        ]
        for expired_key in expired_keys:
            recent_signal_keys.pop(expired_key, None)

        if key in recent_signal_keys and now_ts - recent_signal_keys[key] <= DUPLICATE_WINDOW_SECONDS:
            return True

        recent_signal_keys[key] = now_ts
        return False


def is_line_webhook_payload(data):
    return isinstance(data.get("events"), list)


def extract_line_target_from_event(event):
    source = event.get("source") or {}
    source_type = str(source.get("type", "")).strip()

    if source_type == "user":
        return source_type, source.get("userId")
    if source_type == "group":
        return source_type, source.get("groupId")
    if source_type == "room":
        return source_type, source.get("roomId")

    return source_type, None


def handle_line_webhook_payload(data):
    events = data.get("events") or []
    saved_count = 0
    seen_count = 0

    for event in events:
        if not isinstance(event, dict):
            continue
        target_type, target_id = extract_line_target_from_event(event)
        if not target_id:
            continue
        seen_count += 1
        if save_line_target(
            target_type=target_type,
            target_id=target_id,
            event_type=event.get("type", "")
        ):
            saved_count += 1

    return {
        "status": "processed",
        "kind": "line_webhook",
        "events": len(events),
        "targets_seen": seen_count,
        "targets_saved": saved_count
    }


def is_bo_signal_payload(data):
    signal = str(data.get("signal", "")).strip().upper()
    return signal in ("HIGH", "LOW")


reference_engine = None
reference_engine_lock = threading.Lock()
reference_archive_lock = threading.Lock()


def archive_reference_events(events):
    # Separate append-only records; never update the legacy actual-looking history.
    headers = ["audit_id", "event_id", "recorded_at", "pair", "direction", "status",
               "entry_time_ms", "signal_price", "result_price", "reference_result",
               "actual_result", "source", "notification_sent", "delay_seconds", "details"]
    with reference_archive_lock:
        sheet = get_or_create_worksheet("参考シグナルV22", headers)
        rows = [[event.get(key, "") for key in
                 ("audit_id", "event_id", "recorded_at", "pair", "signal", "status",
                  "entry_time_ms", "signal_price", "result_price", "reference_result",
                  "actual_result", "ticker", "notification_sent", "delay_seconds")]
                + [json.dumps(event, ensure_ascii=False)] for event in events]
        retry_google_operation("APPEND REFERENCE AUDIT", lambda: sheet.append_rows(rows, value_input_option="RAW"))


def get_reference_engine():
    global reference_engine
    with reference_engine_lock:
        if reference_engine is None:
            reference_engine = ReferenceSignals(
                os.getenv("SIGNAL_STATE_PATH", "instance/reference_signals.sqlite3"),
                send_notification_message, archive_reference_events)
            reference_engine.start()
        return reference_engine


def handle_received_signal(data, received_at):
    if data.get("schema_version") == 2:
        allowed = data.get("test") is True or theoption_hours_status(data.get("pair"), received_at)["allowed"]
        return get_reference_engine().handle(data, allowed=allowed)
    try:
        pair = data.get("pair", "USDJPY")
        hours_status = theoption_hours_status(pair, received_at)
        if not hours_status["allowed"]:
            log("THEOPTION HOURS SKIPPED:", hours_status, data)
            return {
                "status": "skipped",
                "reason": "theoption_hours",
                "hours": hours_status
            }

        if is_duplicate_signal(data, received_at):
            log("DUPLICATE SIGNAL SKIPPED:", data)
            return {
                "status": "skipped",
                "reason": "duplicate"
            }

        if should_apply_loss_guard(data):
            guard_status = get_loss_guard_status(pair, received_at)
            if not guard_status["allowed"]:
                log("LOSS GUARD SKIPPED:", guard_status, data)
                notification_sent = notify_loss_guard_block(data, guard_status)
                return {
                    "status": "blocked",
                    "reason": "loss_guard",
                    "notification_sent": notification_sent,
                    "notification_delivery": get_last_notification_delivery_result(),
                    "guard": guard_status
                }

        if is_pre_entry_notice(data):
            if is_pre_entry_terminal_notice(data):
                mark_pre_entry_finished(data, get_notice_type(data).lower())
            auto_cancel_state_id = None
            if is_pre_entry_start_notice(data) and not is_test_payload(data):
                auto_cancel_state_id = start_pre_entry_auto_cancel(data, received_at)
            notification_sent = process_pre_entry_notice(data)
            return {
                "status": "processed",
                "kind": "pre_entry",
                "notification_sent": notification_sent,
                "notification_delivery": get_last_notification_delivery_result(),
                "pre_entry_auto_cancel_state_id": auto_cancel_state_id
            }

        # Old alerts contain unresolved placeholders and have no verifiable clock.
        # Fail closed until their TradingView snapshots have been replaced.
        mark_pre_entry_finished(data, "legacy_rejected")
        rejected = dict(data, notice="PRE_ENTRY_CANCEL", reason="旧形式の時刻を検証できないため中止。V22対応アラートへ更新が必要です")
        notification_sent = process_pre_entry_notice(rejected)
        return {
            "status": "processed",
            "kind": "legacy_rejected",
            "notification_sent": notification_sent,
            "notification_delivery": get_last_notification_delivery_result()
        }

    except Exception as e:
        log_error("WEBHOOK WORKER", e)
        notify_error("Webhookバックグラウンド処理エラー", e)
        return {
            "status": "error",
            "message": str(e)
        }


@app.route("/")
def home():
    return f"BO Signal Bot Running - {APP_VERSION}"


@app.route("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "reference_protocol": get_reference_engine().status(),
        "notification_channel": "discord",
        "notification_config": discord_config_status(),
        "notification_delivery_warnings": discord_delivery_warnings(),
        "notification_delivery": get_last_notification_delivery_result(),
        "discord_config": discord_config_status(),
        "discord_api_retry": discord_api_retry_config_status(),
        "pre_entry_auto_cancel": pre_entry_auto_cancel_status(),
        "loss_guard": risk_guard_config_status(),
        "google_sheets_retry": google_api_retry_config_status(),
        "theoption_hours": theoption_hours_status("USDJPY"),
        "theoption_hours_by_pair": {
            "USDJPY": theoption_hours_status("USDJPY"),
            "BTCUSD": theoption_hours_status("BTCUSD")
        }
    }, 200


@app.route("/line-test", methods=["POST"])
def line_test():
    test_secret = os.getenv("DISCORD_TEST_SECRET") or os.getenv("LINE_TEST_SECRET")
    if not test_secret:
        return {
            "status": "disabled",
            "reason": "DISCORD_TEST_SECRET is not set",
            "notification_channel": "discord",
            "notification_config": discord_config_status(),
            "notification_delivery_warnings": discord_delivery_warnings()
        }, 404

    provided_secret = (
        request.headers.get("X-Discord-Test-Secret")
        or request.headers.get("X-Line-Test-Secret")
        or request.args.get("secret")
    )
    if provided_secret != test_secret:
        return {"status": "forbidden"}, 403

    data = request.get_json(silent=True) or {}
    message = str(data.get("message", "")).strip() or (
        "Discord test from BO Signal Bot\n"
        f"version: {APP_VERSION}\n"
        f"time: {datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S')}"
    )

    if send_notification_message(message):
        return {
            "status": "sent",
            "notification_channel": "discord",
            "notification_config": discord_config_status(),
            "notification_delivery": get_last_notification_delivery_result()
        }, 200

    return {
        "status": "error",
        "notification_channel": "discord",
        "notification_config": discord_config_status(),
        "notification_delivery": get_last_notification_delivery_result()
    }, 500


@app.route("/discord-test", methods=["POST"])
def discord_test():
    return line_test()


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(silent=True)
        if data is None and request.data:
            try:
                data = json.loads(request.data.decode("utf-8"))
            except Exception:
                data = None

        if not isinstance(data, dict):
            log("INVALID PAYLOAD SKIPPED:", data)
            return {
                "status": "accepted",
                "message": "Invalid payload skipped."
            }, 200

        received_at = datetime.now(JST)

        if is_line_webhook_payload(data):
            log("RECEIVED LINE WEBHOOK:", {"events": len(data.get("events") or [])})
            result = handle_line_webhook_payload(data)
            return {
                "status": "accepted",
                "message": "LINE webhook processed.",
                "result": result,
                "notification_channel": "discord",
                "notification_config": discord_config_status()
            }, 200

        if not is_bo_signal_payload(data):
            log("NON SIGNAL PAYLOAD SKIPPED:", data)
            return {
                "status": "accepted",
                "message": "Non-signal payload skipped."
            }, 200

        log("RECEIVED:", data)

        if data.get("schema_version") == 2 and data.get("test") is True:
            return {"status": "processed", "result": handle_received_signal(data, received_at)}, 200

        if is_test_payload(data):
            if not is_pre_entry_notice(data):
                notification_sent = process_test_entry_notice(data)
                result = {
                    "status": "processed",
                    "kind": "entry_test",
                    "notification_sent": notification_sent,
                    "notification_delivery": get_last_notification_delivery_result()
                }
            else:
                result = handle_received_signal(data, received_at)
            return {
                "status": "processed",
                "message": "TEST webhook processed synchronously.",
                "result": result
            }, 200

        worker = threading.Thread(
            target=handle_received_signal,
            args=[data, received_at]
        )
        worker.daemon = True
        worker.start()

        return {
            "status": "accepted",
            "message": "Webhook受信。バックグラウンド処理開始。"
        }, 200

    except Exception as e:
        log_error("WEBHOOK", e)
        return {
            "status": "error",
            "message": str(e)
        }, 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
