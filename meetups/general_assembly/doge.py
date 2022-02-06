import requests as re
import pandas as pd
from datetime import datetime, timedelta

from prefect import task, Flow, case, Parameter
from prefect.tasks.notifications import SlackTask
from prefect.schedules import clocks, Schedule


def format_url(coin) -> str:
   url = "https://production.api.coindesk.com/v2/price/values/"
   start_time = (datetime.now() - timedelta(minutes=10)).isoformat(timespec="minutes")
   end_time = datetime.now().isoformat(timespec="minutes")
   params = f"?start_date={start_time}&end_date={end_time}&ohlc=false"
   return url + coin + params

@task(max_retries=3, retry_delay=timedelta(seconds=10))
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

slack_task = SlackTask("Buy some DOGE now!")

now = datetime.utcnow()
clock1 = clocks.IntervalClock(start_date=now,
               interval=timedelta(minutes=5),
               parameter_defaults={"coin": "DOGE"})

clock2 = clocks.IntervalClock(start_date=now,
               interval=timedelta(minutes=5),
               parameter_defaults={"coin": "BTC"})

clock3 = clocks.IntervalClock(start_date=now,
               interval=timedelta(minutes=5),
               parameter_defaults={"coin": "ETH"})

schedule = Schedule(clocks=[clock1, clock2, clock3])


with Flow("flow_name", schedule=schedule) as flow:
    coin = Parameter("coin", default="DOGE")
    data = get_data(coin)
    dip = detect_dip(data, 0)
    with case(dip,True):
        slack_task()

flow.register("general_assembly")