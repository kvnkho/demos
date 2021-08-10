import prefect
from prefect.executors.dask import LocalDaskExecutor
import requests as re
import pandas as pd
from datetime import datetime, timedelta

from prefect import task, Flow, Parameter, unmapped
from prefect.tasks.notifications import SlackTask
from prefect.executors import LocalDaskExecutor


def format_url(coin="DOGE"):
    url = "https://production.api.coindesk.com/v2/price/values/"
    start_time = (datetime.now() - timedelta(minutes=60)).isoformat(timespec="minutes")
    end_time = datetime.now().isoformat(timespec="minutes")
    params = f"?start_date={start_time}&end_date={end_time}&ohlc=false"
    return url + coin + params

@task(max_retries = 3, retry_delay=timedelta(minutes=1))
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
def get_models():
    from sklearn.linear_model import LinearRegression, Ridge, Lasso
    m1 = LinearRegression()
    m2 = Ridge()
    m3 = Lasso()
    return [m1, m2, m3]

@task
def train_model(df, model):
    from sklearn.metrics import mean_absolute_error
    df['lag1'] = df["price"].shift(1)
    df['lag2'] = df["price"].shift(2)
    df.fillna(0, inplace=True)
    model.fit(df.drop("price", axis=1), df["price"])

    error = mean_absolute_error(model.predict(df.drop("price", axis=1)), df["price"])
    logger = prefect.context.get("logger")
    logger.info(model.__class__.__name__)
    logger.info(error)

    SlackTask(message=f"The model {model.__class__.__name__} had an MAE of {error}", 
    webhook_secret="SLACK_WEBHOOK_URL").run()

    return 

with Flow("to-the-moon-modelling") as flow:
    coin = Parameter("coin", default="DOGE")
    data = get_data(coin)
    models = get_models()
    train_model.map(unmapped(data), models)

flow.executor = LocalDaskExecutor()
flow.register("dsdc")