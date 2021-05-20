import requests as re
import pandas as pd
from datetime import datetime, timedelta
from typing import Callable
import time

from prefect import tasks, Flow


def format_url(coin="DOGE"):
    url = "https://production.api.coindesk.com/v2/price/values/"
    end_time = datetime.now()
    start_time = (end_time - timedelta(minutes=10)).isoformat(timespec="minutes")
    end_time = end_time.isoformat(timespec="minutes")
    params = f"?start_date={start_time}&end_date={end_time}&ohlc=false"
    return url + coin + params

@task
def get_data(coin="DOGE") -> pd.DataFrame:
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

@task
def post_to_slack():
    pass

with Flow("to-the-moon") as flow:
    data = get_data()
    is_dip = detect_dip(data)
    post_to_slack()
    