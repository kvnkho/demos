import requests as re
import pandas as pd
from datetime import datetime

from prefect import task, Flow

def format_url(coin="DOGE"):
    url = "https://production.api.coindesk.com/v2/price/values/"
    params = "?start_date=2021-05-18T15:40&end_date=2021-05-19T03:40&ohlc=false"
    return url + coin + params

@task(max_retries=2, retry_delay=datetime.timedelta(minutes=1))
def get_data(coin="DOGE") -> pd.DataFrame:
    prices = re.get(format_url(coin))
    prices = prices.json()['data']['entries']
    data = pd.DataFrame(prices, columns=["time", "price"])
    return data

def detect_dip(data):
    pct_change = data['price'].max() - data['price'].min()
    return True

def save_plots(): 
data = get_data()
print(data.head())