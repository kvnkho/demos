import os
os.environ["SLACK_WEBHOOK_URL"] = "https://hooks.slack.com/services/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"

import requests as re
import pandas as pd
from datetime import datetime, timedelta
import os

def format_url(coin) -> str:
   url = "https://production.api.coindesk.com/v2/price/values/"
   start_time = (datetime.now() - timedelta(minutes=10)).isoformat(timespec="minutes")
   end_time = datetime.now().isoformat(timespec="minutes")
   params = f"?start_date={start_time}&end_date={end_time}&ohlc=false"
   return url + coin + params

def get_data(coin: str="DOGE"):
    prices = re.get(format_url(coin)).json()["data"]["entries"]
    data = pd.DataFrame(prices, columns=["time", "price"])
    return data

def detect_dip(data, threshold = 10):
    peak = data['price'].max()
    bottom = data['price'].min()
    dip = 100 - (bottom/peak) * 100

    if dip > threshold:
        return True
    else:
        return False

def send_to_slack(message: str):
    r = re.post(
        os.environ["SLACK_WEBHOOK_URL"],
        json=message if isinstance(message, dict) else {"text": message},
    )
    r.raise_for_status()
    return

def check_dip(coin="DOGE", threshold: float=0):
    data = get_data(coin)
    dip = detect_dip(data, threshold)
    if dip == True:
        send_to_slack(f"{coin} has a dip")

check_dip()