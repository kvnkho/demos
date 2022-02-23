import requests as re
import pandas as pd
from datetime import datetime, timedelta
import os
os.environ["SLACK_WEBHOOK_URL"] = "https://hooks.slack.com/services/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"

from prefect import task, flow
from prefect.task_runners import DaskTaskRunner

def format_url(coin) -> str:
   url = "https://production.api.coindesk.com/v2/price/values/"
   start_time = (datetime.now() - timedelta(minutes=10)).isoformat(timespec="minutes")
   end_time = datetime.now().isoformat(timespec="minutes")
   params = f"?start_date={start_time}&end_date={end_time}&ohlc=false"
   return url + coin + params

@task(retries=3, retry_delay_seconds=10)
def get_data(coin: str="DOGE"):
    prices = re.get(format_url(coin)).json()["data"]["entries"]
    data = pd.DataFrame(prices, columns=["time", "price"])
    return data

@task
def detect_dip(data, threshold = 10):
    peak = data['price'].max()
    bottom = data['price'].min()
    dip = 100 - (bottom/peak) * 100

    if dip > threshold:
        return True
    else:
        return False

@task
def send_to_slack(message: str):
    r = re.post(
        os.environ["SLACK_WEBHOOK_URL"],
        json={"text": message},
    )
    r.raise_for_status()
    return

@flow(name="Check Dip", 
task_runner=DaskTaskRunner(
    cluster_kwargs={"n_workers": 4, "threads_per_worker": 2}
))
def check_dip(coins=["DOGE", "BTC", "ETH"], threshold: float=0):
    for coin in coins:
        data = get_data(coin)
        dip = detect_dip(data, threshold)
        if dip.wait().result() == True:
            send_to_slack(f"{coin} has a dip")

if __name__ == "__main__":
    check_dip()