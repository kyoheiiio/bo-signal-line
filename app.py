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

JST = ZoneInfo("Asia/Tokyo")


def send_line_message(message):
    url = "https://api.line.me/v2/bot/message/broadcast"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_ACCESS_TOKEN}"
    }

    payload = {
        "messages": [
            {
                "type": "text",
                "text": message
            }
        ]
    }

    response = requests.post(url, headers=headers, json=payload)

    print("LINE STATUS:", response.status_code)
    print("LINE RESPONSE:", response.text)


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
    sheet = client.open_by_key(SPREADSHEET_ID).sheet1

    return sheet


def append_signal_to_sheet(pair, timeframe, signal, entry_time, judge_time):
    sheet = get_sheet()

    now = datetime.now(JST).strftime("%Y/%m/%d %H:%M:%S")

    row = [
        now,
        pair,
        timeframe,
        signal,
        entry_time,
        judge_time,
        "判定待ち"
    ]

    sheet.append_row(row, value_input_option="USER_ENTERED")

    print("SHEET APPENDED:", row)


def send_entry_message(signal, pair, timeframe):
    message = (
        f"🔴【本番エントリー通知】\n\n"
        f"今すぐエントリー\n\n"
        f"通貨: {pair}\n"
        f"足種: {timeframe}\n"
        f"方向: {signal}"
    )

    send_line_message(message)


@app.route("/")
def home():
    return "BO Signal Bot Running"


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)

        print("RECEIVED:", data)

        signal = data.get("signal", "UNKNOWN")
        pair = data.get("pair", "UNKNOWN")
        timeframe = data.get("timeframe", "UNKNOWN")

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

        append_signal_to_sheet(
            pair=pair,
            timeframe=timeframe,
            signal=signal,
            entry_time=entry_time,
            judge_time=judge_time
        )

        timer = threading.Timer(
            120,
            send_entry_message,
            args=[signal, pair, timeframe]
        )

        timer.daemon = True
        timer.start()

        return {
            "status": "success",
            "message": "LINE通知とスプレッドシート記録完了"
        }, 200

    except Exception as e:
        print("ERROR:", str(e))

        return {
            "status": "error",
            "message": str(e)
        }, 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))

    app.run(
        host="0.0.0.0",
        port=port
    )
