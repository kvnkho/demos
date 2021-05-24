import requests as re
import pandas as pd
from datetime import datetime, timedelta
from typing import Callable
import time

def format_url(coin: str="DOGE") -> str:
    url = "https://production.api.coindesk.com/v2/price/values/"
    start_time = (datetime.now() - timedelta(minutes=10)).isoformat(timespec="minutes")
    end_time = datetime.now().isoformat(timespec="minutes")
    params = f"?start_date={start_time}&end_date={end_time}&ohlc=false"
    return url + coin + params

def get_data(coin: str="DOGE") -> pd.DataFrame:
    prices = re.get(format_url(coin))
    prices = prices.json()['data']['entries']
    data = pd.DataFrame(prices, columns=["time", "price"])
    return data

def detect_dip(data, threshold = 10):
    peak = data['price'].max()
    bottom = data['price'].min()
    dip = 100 - (bottom/peak)*100
    if dip > threshold:
        return True
    else:
        return False

def post_to_slack():
    pass

while True:
    time.sleep(300)
    data = get_data()
    is_dip = detect_dip(data)
    if is_dip:
        post_to_slack()