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
SYMBOL = "USD/JPY"
HISTORY_SHEET_NAME = "履歴"
SUMMARY_SHEET_NAME = "日別集計"
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


def send_line_message(message):
    url = "https://api.line.me/v2/bot/message/broadcast"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_ACCESS_TOKEN}"
    }
    payload = {"messages": [{"type": "text", "text": message}]}
    response = requests.post(url, headers=headers, json=payload, timeout=10)
    print("LINE STATUS:", response.status_code)
    print("LINE RESPONSE:", response.text)


def get_usdjpy_price():
    url = "https://api.twelvedata.com/price"
    params = {
        "symbol": SYMBOL,
        "apikey": TWELVE_DATA_API_KEY
    }
    response = requests.get(url, params=params, timeout=10)
    data = response.json()
    print("PRICE RESPONSE:", data)

    if "price" not in data:
        raise Exception(f"価格取得失敗: {data}")

    return float(data["price"])


def get_spreadsheet():
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


def get_sheet():
    spreadsheet = get_spreadsheet()

    try:
        return spreadsheet.worksheet(HISTORY_SHEET_NAME)
    except WorksheetNotFound:
        return spreadsheet.sheet1


def update_values(worksheet, range_name, values):
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


def get_or_create_summary_sheet():
    spreadsheet = get_spreadsheet()

    try:
        sheet = spreadsheet.worksheet(SUMMARY_SHEET_NAME)
    except WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(
            title=SUMMARY_SHEET_NAME,
            rows=1000,
            cols=len(SUMMARY_HEADERS)
        )

    update_values(sheet, "A1:H1", [SUMMARY_HEADERS])

    try:
        sheet.format("H2:H", {
            "numberFormat": {
                "type": "PERCENT",
                "pattern": "0.0%"
            }
        })
    except Exception as e:
        print("SUMMARY FORMAT SKIPPED:", str(e))

    return sheet


def update_daily_summary():
    history_sheet = get_sheet()
    summary_sheet = get_or_create_summary_sheet()
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
        print("SUMMARY FORMAT SKIPPED:", str(e))

    print("DAILY SUMMARY UPDATED")


def safe_update_daily_summary():
    try:
        update_daily_summary()
    except Exception as e:
        print("DAILY SUMMARY ERROR:", str(e))


def append_pending_row(pair, timeframe, signal, entry_time, judge_time):
    sheet = get_sheet()
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

    print("SHEET APPENDED ROW:", row_number)

    return row_number


def update_entry_price(row_number, entry_price):
    sheet = get_sheet()
    sheet.update_cell(row_number, 6, entry_price)
    print("ENTRY PRICE UPDATED:", entry_price)


def update_cancel(row_number, entry_price):
    sheet = get_sheet()
    sheet.update_cell(row_number, 6, entry_price)
    sheet.update_cell(row_number, 9, "CANCEL")
    safe_update_daily_summary()
    print("ENTRY CANCELLED:", entry_price)


def update_result(row_number, judge_price, result):
    sheet = get_sheet()
    sheet.update_cell(row_number, 8, judge_price)
    sheet.update_cell(row_number, 9, result)
    print("RESULT UPDATED:", judge_price, result)


def judge_result(signal, entry_price, judge_price):
    if signal == "HIGH":
        if judge_price > entry_price:
            return "WIN"
        elif judge_price < entry_price:
            return "LOSE"
        else:
            return "DRAW"

    if signal == "LOW":
        if judge_price < entry_price:
            return "WIN"
        elif judge_price > entry_price:
            return "LOSE"
        else:
            return "DRAW"

    return "UNKNOWN"


def should_cancel_entry(signal, signal_price, entry_price):
    if signal_price is None:
        return False

    try:
        signal_price = float(signal_price)
    except Exception:
        return False

    if signal == "HIGH" and entry_price <= signal_price:
        return True

    if signal == "LOW" and entry_price >= signal_price:
        return True

    return False


def send_entry_and_schedule_judgement(signal, pair, timeframe, row_number, signal_price):
    try:
        entry_price = get_usdjpy_price()

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

            send_line_message(cancel_message)
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

        send_line_message(message)

        timer = threading.Timer(
            300,
            judge_and_update_sheet,
            args=[signal, pair, timeframe, row_number, entry_price]
        )
        timer.daemon = True
        timer.start()

    except Exception as e:
        print("ENTRY ERROR:", str(e))
        send_line_message(f"⚠️ エントリー処理エラー\n{str(e)}")


def judge_and_update_sheet(signal, pair, timeframe, row_number, entry_price):
    try:
        judge_price = get_usdjpy_price()
        result = judge_result(signal, entry_price, judge_price)

        update_result(row_number, judge_price, result)
        safe_update_daily_summary()

        message = (
            f"📊【判定結果】\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n\n"
            f"エントリー価格: {entry_price}\n"
            f"判定終了価格: {judge_price}\n"
            f"結果: {result}"
        )

        send_line_message(message)

    except Exception as e:
        print("JUDGE ERROR:", str(e))
        send_line_message(f"⚠️ 判定処理エラー\n{str(e)}")


def process_signal(data):
    try:
        print("PROCESS START:", data)

        signal = data.get("signal", "UNKNOWN")
        pair = data.get("pair", "USDJPY")
        timeframe = data.get("timeframe", "5")
        signal_price = data.get("signal_price")

        now = datetime.now(JST)
        entry_dt = now + timedelta(minutes=2)
        judge_dt = entry_dt + timedelta(minutes=5)

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

        send_line_message(pre_message)

        row_number = append_pending_row(
            pair=pair,
            timeframe=timeframe,
            signal=signal,
            entry_time=entry_time,
            judge_time=judge_time
        )

        timer = threading.Timer(
            120,
            send_entry_and_schedule_judgement,
            args=[signal, pair, timeframe, row_number, signal_price]
        )
        timer.daemon = True
        timer.start()

    except Exception as e:
        print("PROCESS ERROR:", str(e))
        send_line_message(f"⚠️ シグナル処理エラー\n{str(e)}")


@app.route("/")
def home():
    return "BO Signal Bot Running"


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)

        print("RECEIVED:", data)

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
        print("WEBHOOK ERROR:", str(e))

        return {
            "status": "error",
            "message": str(e)
        }, 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
