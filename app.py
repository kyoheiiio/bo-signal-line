from flask import Flask, request
import requests
import os
import threading

app = Flask(__name__)

LINE_ACCESS_TOKEN = os.getenv("LINE_ACCESS_TOKEN")


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

        pre_message = (
            f"🟡【事前通知｜2分前】\n\n"
            f"まだエントリーしない\n\n"
            f"通貨: {pair}\n"
            f"足種: {timeframe}\n"
            f"方向: {signal}\n\n"
            f"2分後にエントリー予定"
        )

        send_line_message(pre_message)

        timer = threading.Timer(
            120,
            send_entry_message,
            args=[signal, pair, timeframe]
        )

        timer.daemon = True
        timer.start()

        return {
            "status": "success",
            "message": "2分前通知送信、エントリー通知予約完了"
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
