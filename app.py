from flask import Flask, request

app = Flask(__name__)

@app.route("/")
def home():
    return "BO Signal Bot Running"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print(data)
    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
