
import requests as re
import pandas as pd
from datetime import datetime, timedelta

from prefect import task, flow

def format_url(coin="DOGE"):
    url = "https://production.api.coindesk.com/v2/price/values/"
    start_time = (datetime.now() - timedelta(minutes=10)).isoformat(timespec="minutes")
    end_time = datetime.now().isoformat(timespec="minutes")
    params = f"?start_date={start_time}&end_date={end_time}&ohlc=false"
    return url + coin + params

@task
def get_data(coin="DOGE") -> pd.DataFrame:
    prices = re.get(format_url(coin))
    prices = prices.json()['data']['entries']
    data = pd.DataFrame(prices, columns=["time", "price"])
    return data

@task
def detect_dip(df: pd.DataFrame, threshold):
    peak = df['price'].max()
    bottom = df['price'].min()
    dip = 100 - (bottom/peak)*100
    
    if dip > threshold:
        return True
    else:
        return False

@task
def reduce_dips(dips):
    return max(dips)

@task
def post_to_slack():
    r = re.post(
        "https://hooks.slack.com/services/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",
        json={"text": "test"},
    )
    r.raise_for_status()
    return

@flow
def run_all(coin="DOGE", threshold=0):
    data = get_data(coin)
    is_dip = detect_dip(data, threshold=threshold)
    if is_dip.wait().result():
        post_to_slack()

run_all()