from flask import Flask, request
import requests
import os

app = Flask(__name__)

LINE_ACCESS_TOKEN = "ここに新しいチャネルアクセストークン"

def send_line_message(message):
    url = "https://api.line.me/v2/bot/message/broadcast"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_ACCESS_TOKEN}"
    }

    data = {
        "messages": [
            {
                "type": "text",
                "text": message
            }
        ]
    }

    requests.post(url, headers=headers, json=data)

@app.route("/")
def home():
    return "BO Signal Bot Running"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json

    signal = data.get("signal", "SIGNAL")
    pair = data.get("pair", "UNKNOWN")
    timeframe = data.get("timeframe", "UNKNOWN")

    message = f"""
📈 BO SIGNAL

通貨: {pair}
足種: {timeframe}
シグナル: {signal}
"""

    send_line_message(message)

    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
