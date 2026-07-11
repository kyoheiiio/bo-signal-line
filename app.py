from flask import Flask, request
import requests
import os
import threading
import json
import gspread
from collections import defaultdict
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

app = Flask(__name__)

APP_VERSION = "immediate entry v6 pre entry status notice"

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

JST = ZoneInfo("Asia/Tokyo")

JUDGE_DELAY_SECONDS = 300
SYMBOL = "USD/JPY"
HISTORY_SHEET_NAME = "履歴"
SUMMARY_SHEET_NAME = "日別集計"
DUPLICATE_WINDOW_SECONDS = 120
THEOPTION_HOURS_FILTER_ENABLED = os.getenv(
    "THEOPTION_HOURS_FILTER_ENABLED",
    "true"
).strip().lower() not in ("0", "false", "no", "off")
THEOPTION_START_HOUR = 8
THEOPTION_END_HOUR = 6

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

recent_signal_keys = {}
recent_signal_lock = threading.Lock()


def log(message, *values):
    if values:
        print(message, *values, flush=True)
    else:
        print(message, flush=True)


def log_error(stage, error):
    log(f"ERROR [{stage}]: {type(error).__name__}: {error}")


log("APP VERSION:", APP_VERSION)


def get_line_access_token():
    return (
        os.getenv("LINE_ACCESS_TOKEN")
        or os.getenv("LINE_CHANNEL_ACCESS_TOKEN")
        or os.getenv("LINE_BOT_CHANNEL_ACCESS_TOKEN")
        or os.getenv("LINE_BOT_TOKEN")
        or os.getenv("LINE_CHANNEL_TOKEN")
    )


def get_line_targets():
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


def line_config_status():
    token_exists = bool(get_line_access_token())
    targets = get_line_targets()
    return {
        "line_token": token_exists,
        "line_to": bool(targets),
        "line_target_count": len(targets),
        "delivery_mode": (
            "push" if len(targets) == 1
            else "multicast" if len(targets) > 1
            else "broadcast" if token_exists
            else "not_configured"
        )
    }


def get_theoption_session_window(current_dt):
    if current_dt.tzinfo is None:
        current_dt = current_dt.replace(tzinfo=JST)

    if current_dt.hour >= THEOPTION_START_HOUR:
        session_day = current_dt.date()
    elif current_dt.hour < THEOPTION_END_HOUR:
        session_day = (current_dt - timedelta(days=1)).date()
    else:
        return None

    session_start = datetime(
        session_day.year,
        session_day.month,
        session_day.day,
        THEOPTION_START_HOUR,
        0,
        0,
        tzinfo=JST
    )
    session_end = session_start + timedelta(days=1)
    session_end = session_end.replace(hour=THEOPTION_END_HOUR, minute=0, second=0, microsecond=0)
    return session_start, session_end


def theoption_hours_status(received_at=None, judge_delay_seconds=JUDGE_DELAY_SECONDS):
    received_at = received_at or datetime.now(JST)
    if received_at.tzinfo is None:
        received_at = received_at.replace(tzinfo=JST)
    judge_at = received_at + timedelta(seconds=judge_delay_seconds)
    session_window = get_theoption_session_window(received_at)

    status = {
        "enabled": THEOPTION_HOURS_FILTER_ENABLED,
        "allowed": False,
        "now": received_at.strftime("%Y/%m/%d %H:%M:%S"),
        "judge_time": judge_at.strftime("%Y/%m/%d %H:%M:%S"),
        "session_start": None,
        "session_end": None,
        "reason": ""
    }

    if not THEOPTION_HOURS_FILTER_ENABLED:
        status["allowed"] = True
        status["reason"] = "filter disabled"
        return status

    if session_window is None:
        status["reason"] = "outside theoption USDJPY session"
        return status

    session_start, session_end = session_window
    status["session_start"] = session_start.strftime("%Y/%m/%d %H:%M:%S")
    status["session_end"] = session_end.strftime("%Y/%m/%d %H:%M:%S")

    if session_start.weekday() > 4:
        status["reason"] = "weekend session"
        return status

    if received_at < session_start or received_at >= session_end:
        status["reason"] = "outside theoption USDJPY session"
        return status

    if judge_at >= session_end:
        status["reason"] = "judge time exceeds session close"
        return status

    status["allowed"] = True
    status["reason"] = "inside theoption USDJPY session"
    return status


def send_line_message(message):
    try:
        line_access_token = get_line_access_token()
        if not line_access_token:
            raise RuntimeError("LINE access token is not set")

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {line_access_token}"
        }
        messages = [{"type": "text", "text": message}]
        targets = get_line_targets()

        if len(targets) == 1:
            url = "https://api.line.me/v2/bot/message/push"
            payload = {"to": targets[0], "messages": messages}
        elif len(targets) > 1:
            url = "https://api.line.me/v2/bot/message/multicast"
            payload = {"to": targets, "messages": messages}
        else:
            url = "https://api.line.me/v2/bot/message/broadcast"
            payload = {"messages": messages}

        response = requests.post(url, headers=headers, json=payload, timeout=10)

        log("LINE STATUS:", response.status_code)
        log("LINE RESPONSE:", response.text)

        if response.status_code >= 400:
            raise RuntimeError(f"LINE API error {response.status_code}: {response.text}")

        return True

    except Exception as e:
        log_error("LINE SEND", e)
        return False


def notify_error(title, error):
    message = f"⚠️ {title}\n{type(error).__name__}: {error}"
    send_line_message(message)


def get_usdjpy_price():
    try:
        if not TWELVE_DATA_API_KEY:
            raise RuntimeError("TWELVE_DATA_API_KEY is not set")

        url = "https://api.twelvedata.com/price"
        params = {
            "symbol": SYMBOL,
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
        return client.open_by_key(SPREADSHEET_ID)

    except Exception as e:
        log_error("GOOGLE SHEETS AUTH", e)
        raise


def update_values(worksheet, range_name, values):
    try:
        try:
            worksheet.update(
                range_name=range_name,
                values=values,
                value_input_option="USER_ENTERED"
            )
        except TypeError:
            worksheet.update(
                range_name,
                values,
                value_input_option="USER_ENTERED"
            )
    except Exception as e:
        log_error("GOOGLE SHEETS UPDATE VALUES", e)
        raise


def get_or_create_worksheet(sheet_name, headers):
    try:
        spreadsheet = get_spreadsheet()

        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except WorksheetNotFound:
            worksheet = None

            if sheet_name == HISTORY_SHEET_NAME:
                worksheets = spreadsheet.worksheets()
                if worksheets and worksheets[0].title != SUMMARY_SHEET_NAME:
                    worksheet = worksheets[0]
                    if worksheet.title != HISTORY_SHEET_NAME:
                        worksheet.update_title(HISTORY_SHEET_NAME)

            if worksheet is None:
                worksheet = spreadsheet.add_worksheet(
                    title=sheet_name,
                    rows=1000,
                    cols=len(headers)
                )

        ensure_headers(worksheet, headers)

        return worksheet

    except Exception as e:
        log_error(f"GET OR CREATE WORKSHEET {sheet_name}", e)
        raise


def ensure_headers(worksheet, headers):
    try:
        first_row = worksheet.row_values(1)
        if first_row[:len(headers)] == headers:
            return

        first_row_has_value = any(str(value).strip() for value in first_row)
        first_row_looks_like_header = any(value in headers for value in first_row)

        if first_row_has_value and not first_row_looks_like_header:
            worksheet.insert_row(headers, index=1, value_input_option="USER_ENTERED")
        else:
            update_values(worksheet, f"A1:{chr(64 + len(headers))}1", [headers])

    except Exception as e:
        log_error("ENSURE HEADERS", e)
        raise


def get_history_sheet():
    return get_or_create_worksheet(HISTORY_SHEET_NAME, HISTORY_HEADERS)


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


def update_daily_summary():
    try:
        history_sheet = get_history_sheet()
        summary_sheet = get_or_create_worksheet(SUMMARY_SHEET_NAME, SUMMARY_HEADERS)
        rows = history_sheet.get_all_values()

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

        summary_sheet.clear()
        update_values(summary_sheet, f"A1:H{len(values)}", values)

        try:
            summary_sheet.format("H2:H", {
                "numberFormat": {
                    "type": "PERCENT",
                    "pattern": "0.0%"
                }
            })
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

        sheet.append_row(row, value_input_option="USER_ENTERED")
        row_number = len(sheet.get_all_values())

        log("SHEET APPENDED ROW:", row_number)
        safe_update_daily_summary()

        return row_number

    except Exception as e:
        log_error("SHEET APPEND ENTRY ROW", e)
        raise


def format_price(price):
    return f"{float(price):.3f}"


def format_optional_price(price):
    text = str(price or "").strip()
    if not text:
        return "N/A"

    try:
        return format_price(text)
    except (TypeError, ValueError):
        return text


def update_result(row_number, judge_price, result):
    try:
        sheet = get_history_sheet()
        sheet.update_cell(row_number, 8, judge_price)
        sheet.update_cell(row_number, 9, result)
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
        judge_price = get_usdjpy_price()
        judge_price_text = format_price(judge_price)
        log("JUDGE PRICE:", judge_price)

        result = judge_result(signal, entry_price, judge_price)
        update_result(row_number, judge_price_text, result)

        message = (
            f"📊【判定結果】\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n\n"
            f"エントリー価格: {format_price(entry_price)}\n"
            f"判定終了価格: {judge_price_text}\n"
            f"結果: {result}"
        )

        if send_line_message(message):
            log("JUDGE NOTICE SENT")

    except Exception as e:
        log_error("JUDGE PROCESS", e)
        notify_error("判定処理エラー", e)


def process_signal(data):
    try:
        log("PROCESS START:", data)

        signal = str(data.get("signal", "UNKNOWN")).strip().upper()
        pair = str(data.get("pair", "USDJPY")).strip()
        timeframe = str(data.get("timeframe", "5")).strip()
        signal_price = str(data.get("signal_price", "")).strip()
        signal_price_text = format_optional_price(signal_price)

        log("SIGNAL:", signal)
        log("PAIR:", pair)
        log("TIMEFRAME:", timeframe)
        log("SIGNAL PRICE:", signal_price_text)

        now = datetime.now(JST)
        judge_dt = now + timedelta(seconds=JUDGE_DELAY_SECONDS)

        entry_time = now.strftime("%Y/%m/%d %H:%M:%S")
        judge_time = judge_dt.strftime("%Y/%m/%d %H:%M:%S")

        entry_price = get_usdjpy_price()
        entry_price_text = format_price(entry_price)
        log("ENTRY PRICE:", entry_price)

        row_number = append_entry_row(
            pair=pair,
            timeframe=timeframe,
            signal=signal,
            entry_time=entry_time,
            entry_price=entry_price_text,
            judge_time=judge_time
        )

        message = (
            f"🔴【即エントリー通知】\n\n"
            f"今すぐエントリー\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n"
            f"シグナル時価格: {signal_price_text}\n"
            f"エントリー価格: {entry_price_text}\n"
            f"判定予定時刻: {judge_time}"
        )

        if send_line_message(message):
            log("ENTRY NOTICE SENT")

        timer = threading.Timer(
            JUDGE_DELAY_SECONDS,
            judge_and_update_sheet,
            args=[signal, pair, timeframe, row_number, entry_price]
        )
        timer.daemon = True
        timer.start()

    except Exception as e:
        log_error("SIGNAL PROCESS", e)
        notify_error("シグナル処理エラー", e)


def get_notice_type(data):
    notice = str(data.get("notice", data.get("type", ""))).strip().upper()
    return notice


def is_pre_entry_notice(data):
    return get_notice_type(data) in (
        "PRE_ENTRY",
        "PRE",
        "ENTRY_PREVIEW",
        "PRE_ENTRY_CANCEL",
        "PRE_ENTRY_PENDING"
    )


def process_pre_entry_notice(data):
    try:
        log("PRE ENTRY NOTICE START:", data)

        notice = get_notice_type(data)
        signal = str(data.get("signal", "UNKNOWN")).strip().upper()
        pair = str(data.get("pair", "USDJPY")).strip()
        timeframe = str(data.get("timeframe", "1")).strip()
        signal_price = str(data.get("signal_price", "")).strip()
        signal_price_text = format_optional_price(signal_price)
        alert_time = str(data.get("alert_time", "")).strip()
        reason = str(data.get("reason", "")).strip()
        now = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")

        if notice in ("PRE_ENTRY_CANCEL",):
            title = "⚪【エントリー予告取消】"
            body = (
                "予告後、1分足確定時に条件が崩れました。\n"
                "今回はエントリー見送りです。"
            )
        elif notice in ("PRE_ENTRY_PENDING",):
            title = "🟠【エントリー予告 保留】"
            body = (
                "予告から1分を過ぎましたが、確定エントリー/取消を確認できません。\n"
                "TradingView側の状態を確認してください。"
            )
        else:
            title = "🟡【エントリー予告】"
            body = (
                "まだ確定ではありません。\n"
                "このまま1分足が確定するとエントリー通知になる可能性があります。"
            )

        reason_line = f"\n理由: {reason}" if reason else ""
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

        if send_line_message(message):
            log("PRE ENTRY NOTICE SENT")

    except Exception as e:
        log_error("PRE ENTRY NOTICE", e)
        notify_error("エントリー予告処理エラー", e)


def build_duplicate_key(data, received_at):
    alert_time = str(data.get("alert_time", "")).strip()
    received_second = received_at.strftime("%Y/%m/%d %H:%M:%S")
    signal_time = alert_time or received_second

    return (
        str(data.get("notice", data.get("type", ""))).strip().upper(),
        str(data.get("signal", "")).strip().upper(),
        str(data.get("pair", "")).strip(),
        str(data.get("timeframe", "")).strip(),
        str(data.get("signal_price", "")).strip(),
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


def handle_received_signal(data, received_at):
    try:
        hours_status = theoption_hours_status(received_at)
        if not hours_status["allowed"]:
            log("THEOPTION HOURS SKIPPED:", hours_status, data)
            return

        if is_duplicate_signal(data, received_at):
            log("DUPLICATE SIGNAL SKIPPED:", data)
            return

        if is_pre_entry_notice(data):
            process_pre_entry_notice(data)
            return

        process_signal(data)

    except Exception as e:
        log_error("WEBHOOK WORKER", e)
        notify_error("Webhookバックグラウンド処理エラー", e)


@app.route("/")
def home():
    return f"BO Signal Bot Running - {APP_VERSION}"


@app.route("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "line_config": line_config_status(),
        "theoption_hours": theoption_hours_status()
    }, 200


@app.route("/line-test", methods=["POST"])
def line_test():
    test_secret = os.getenv("LINE_TEST_SECRET")
    if not test_secret:
        return {"status": "disabled"}, 404

    provided_secret = request.headers.get("X-Line-Test-Secret") or request.args.get("secret")
    if provided_secret != test_secret:
        return {"status": "forbidden"}, 403

    data = request.get_json(silent=True) or {}
    message = str(data.get("message", "")).strip() or (
        "LINE test from BO Signal Bot\n"
        f"version: {APP_VERSION}\n"
        f"time: {datetime.now(JST).strftime('%Y/%m/%d %H:%M:%S')}"
    )

    if send_line_message(message):
        return {
            "status": "sent",
            "line_config": line_config_status()
        }, 200

    return {
        "status": "error",
        "line_config": line_config_status()
    }, 500


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

        log("RECEIVED:", data)

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
