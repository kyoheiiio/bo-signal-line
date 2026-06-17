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

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY")

JST = ZoneInfo("Asia/Tokyo")

ENTRY_DELAY_SECONDS = 120
JUDGE_DELAY_SECONDS = 300
SYMBOL = "USD/JPY"
HISTORY_SHEET_NAME = "履歴"
SUMMARY_SHEET_NAME = "日別集計"
DUPLICATE_WINDOW_SECONDS = 120

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


def send_line_message(message):
    try:
        if not LINE_ACCESS_TOKEN:
            raise RuntimeError("LINE_ACCESS_TOKEN is not set")

        url = "https://api.line.me/v2/bot/message/broadcast"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {LINE_ACCESS_TOKEN}"
        }
        payload = {"messages": [{"type": "text", "text": message}]}
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


def append_pending_row(pair, timeframe, signal, entry_time, judge_time):
    try:
        sheet = get_history_sheet()
        now = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")

        row = [
            now,
            pair,
            timeframe,
            signal,
            entry_time,
            "",
            judge_time,
            "",
            "判定待ち"
        ]

        sheet.append_row(row, value_input_option="USER_ENTERED")
        row_number = len(sheet.get_all_values())

        log("SHEET APPENDED ROW:", row_number)
        safe_update_daily_summary()

        return row_number

    except Exception as e:
        log_error("SHEET APPEND PENDING ROW", e)
        raise


def update_entry_price(row_number, entry_price):
    try:
        sheet = get_history_sheet()
        sheet.update_cell(row_number, 6, entry_price)
        log("ENTRY PRICE:", entry_price)
    except Exception as e:
        log_error("SHEET UPDATE ENTRY PRICE", e)
        raise


def update_cancel(row_number, entry_price):
    try:
        sheet = get_history_sheet()
        sheet.update_cell(row_number, 6, entry_price)
        sheet.update_cell(row_number, 9, "CANCEL")
        log("ENTRY CANCELLED:", entry_price)
        safe_update_daily_summary()
    except Exception as e:
        log_error("SHEET UPDATE CANCEL", e)
        raise


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


def should_cancel_entry(signal, signal_price, entry_price):
    if signal_price is None:
        return False

    try:
        signal_price_float = float(signal_price)
    except Exception:
        return False

    if signal == "HIGH" and entry_price <= signal_price_float:
        return True

    if signal == "LOW" and entry_price >= signal_price_float:
        return True

    return False


def send_entry_and_schedule_judgement(signal, pair, timeframe, row_number, signal_price):
    try:
        entry_price = get_usdjpy_price()
        log("ENTRY PRICE:", entry_price)

        if should_cancel_entry(signal, signal_price, entry_price):
            update_cancel(row_number, entry_price)

            cancel_message = (
                f"⚫【エントリー中止】\n\n"
                f"今回は見送り\n\n"
                f"通貨: {pair}\n"
                f"足種: {timeframe}\n"
                f"方向: {signal}\n\n"
                f"シグナル時価格: {signal_price}\n"
                f"2分後価格: {entry_price}\n\n"
                f"理由: 方向が崩れたため"
            )

            if send_line_message(cancel_message):
                log("ENTRY CANCEL NOTICE SENT")
            return

        update_entry_price(row_number, entry_price)

        message = (
            f"🔴【本番エントリー通知】\n\n"
            f"今すぐエントリー\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n"
            f"エントリー価格: {entry_price}"
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
        log_error("ENTRY PROCESS", e)
        notify_error("エントリー処理エラー", e)


def judge_and_update_sheet(signal, pair, timeframe, row_number, entry_price):
    try:
        judge_price = get_usdjpy_price()
        log("JUDGE PRICE:", judge_price)

        result = judge_result(signal, entry_price, judge_price)
        update_result(row_number, judge_price, result)

        message = (
            f"📊【判定結果】\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n\n"
            f"エントリー価格: {entry_price}\n"
            f"判定終了価格: {judge_price}\n"
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
        signal_price = data.get("signal_price")

        now = datetime.now(JST)
        entry_dt = now + timedelta(seconds=ENTRY_DELAY_SECONDS)
        judge_dt = entry_dt + timedelta(seconds=JUDGE_DELAY_SECONDS)

        entry_time = entry_dt.strftime("%Y/%m/%d %H:%M:%S")
        judge_time = judge_dt.strftime("%Y/%m/%d %H:%M:%S")

        pre_message = (
            f"🟡【事前通知｜2分前】\n\n"
            f"まだエントリーしない\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n"
            f"シグナル時価格: {signal_price}\n\n"
            f"エントリー予定時刻: {entry_time}\n"
            f"判定予定時刻: {judge_time}"
        )

        if send_line_message(pre_message):
            log("PRE NOTICE SENT")

        row_number = append_pending_row(
            pair=pair,
            timeframe=timeframe,
            signal=signal,
            entry_time=entry_time,
            judge_time=judge_time
        )

        timer = threading.Timer(
            ENTRY_DELAY_SECONDS,
            send_entry_and_schedule_judgement,
            args=[signal, pair, timeframe, row_number, signal_price]
        )
        timer.daemon = True
        timer.start()

    except Exception as e:
        log_error("SIGNAL PROCESS", e)
        notify_error("シグナル処理エラー", e)


def build_duplicate_key(data, received_at):
    received_minute = received_at.strftime("%Y/%m/%d %H:%M")
    return (
        str(data.get("signal", "")).strip().upper(),
        str(data.get("pair", "")).strip(),
        str(data.get("timeframe", "")).strip(),
        str(data.get("signal_price", "")).strip(),
        received_minute
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


@app.route("/")
def home():
    return "BO Signal Bot Running"


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)
        received_at = datetime.now(JST)

        log("RECEIVED:", data)

        if is_duplicate_signal(data, received_at):
            log("DUPLICATE SIGNAL SKIPPED:", data)
            return {
                "status": "accepted",
                "message": "Duplicate signal skipped."
            }, 200

        worker = threading.Thread(
            target=process_signal,
            args=[data]
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
