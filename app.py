from flask import Flask, request
import requests
import os
import threading
import json
import gspread
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


def send_line_message(message):
    url = "https://api.line.me/v2/bot/message/broadcast"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_ACCESS_TOKEN}"
    }

    payload = {"messages": [{"type": "text", "text": message}]}

    response = requests.post(url, headers=headers, json=payload)

    print("LINE STATUS:", response.status_code)
    print("LINE RESPONSE:", response.text)


def get_usdjpy_price():
    url = "https://api.twelvedata.com/price"

    params = {
        "symbol": SYMBOL,
        "apikey": TWELVE_DATA_API_KEY
    }

    response = requests.get(url, params=params)
    data = response.json()

    print("PRICE RESPONSE:", data)

    if "price" not in data:
        raise Exception(f"価格取得失敗: {data}")

    return float(data["price"])


def get_sheet():
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
    return client.open_by_key(SPREADSHEET_ID).sheet1


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


def send_entry_and_schedule_judgement(signal, pair, timeframe, row_number):
    try:
        entry_price = get_usdjpy_price()
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
        send_line_message(f"⚠️ エントリー価格取得エラー\n{str(e)}")


def judge_and_update_sheet(signal, pair, timeframe, row_number, entry_price):
    try:
        judge_price = get_usdjpy_price()
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

        send_line_message(message)

    except Exception as e:
        print("JUDGE ERROR:", str(e))
        send_line_message(f"⚠️ 判定価格取得エラー\n{str(e)}")


@app.route("/")
def home():
    return "BO Signal Bot Running"


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)

        print("RECEIVED:", data)

        signal = data.get("signal", "UNKNOWN")
        pair = data.get("pair", "USDJPY")
        timeframe = data.get("timeframe", "5")

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
            f"方向: {signal}\n\n"
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
            args=[signal, pair, timeframe, row_number]
        )

        timer.daemon = True
        timer.start()

        return {
            "status": "success",
            "message": "LINE通知、価格取得予約、スプレッドシート記録完了"
        }, 200

    except Exception as e:
        print("ERROR:", str(e))

        return {
            "status": "error",
            "message": str(e)
        }, 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))

    app.run(host="0.0.0.0", port=port)
