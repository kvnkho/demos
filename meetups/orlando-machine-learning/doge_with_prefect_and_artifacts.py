import tempfile

import requests as re
import pandas as pd
from datetime import datetime, timedelta
import coiled
from prefect.tasks.control_flow.case import case
import matplotlib.pyplot as plt
import seaborn as sns
import io

from prefect import task, Flow, Parameter, unmapped
from prefect.tasks.notifications import SlackTask
from prefect.executors import DaskExecutor
import prefect
from prefect.engine.results import S3Result
from prefect.engine.serializers import Serializer

MARKDOWN = """# {coin_name}

{summary}

<img src=" https://omlds-prefect.s3.amazonaws.com/{image}">
"""

class NoOpSerializer(Serializer):
    """A `Serializer` that does nothing."""

    def serialize(self, value):
        return value

    def deserialize(self, value):
        return value

def format_url(coin="DOGE"):
    url = "https://production.api.coindesk.com/v2/price/values/"
    start_time = (datetime.now() - timedelta(minutes=40)).isoformat(timespec="minutes")
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
def detect_dip(df: pd.DataFrame, threshold, coin_name):
    from statsmodels.tsa.arima.model import ARIMA
    model = ARIMA(df['price'], order=(2,2,1))
    model_fit = model.fit()

    # summary of fit model
    summary = model_fit.summary()

    # predictions
    df['set'] = 'train'
    df2 = pd.DataFrame({'price': model_fit.forecast(5)})
    df2['set'] = 'forecast'
    df = df[['price', 'set']]
    df = pd.concat([df, df2], axis=0, ignore_index=True)
    
    plt.figure()
    sns.lineplot(list(range(0,len(df['price']))), df['price'], hue=df['set'])
    plt.title(f"{coin_name} Forecast")
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png',bbox_inches='tight')
    buffer.seek(0)

    my_result = S3Result(bucket="omlds-prefect", location="{flow_run_name}/" + coin_name + "/forecast.png", serializer=NoOpSerializer())
    res = my_result.write(buffer.getvalue(), **prefect.context)
    
    prefect.artifacts.create_markdown(MARKDOWN.format(coin_name=coin_name, summary=summary,
        image=res.location))
    
    return True

post_to_slack = SlackTask(message="There has been a dip in crypto price.", webhook_secret="SLACK_WEBHOOK_URL")

with Flow("to-the-moon-with-ARIMA") as flow:
    coin = Parameter("coin", default=["DOGE", "BTC", "ETH"])
    threshold = Parameter("threshold", default=0)
    data = get_data.map(coin)
    is_dip = detect_dip.map(data, unmapped(threshold), coin)
    dip = task(lambda x: max(x))(is_dip)
    with case(dip, True):
        post_to_slack()

# # Create a software environment for our workers
# coiled.create_software_environment(
#     name="prefect",
#     conda={"channels": ["conda-forge"],
#             "dependencies": ["python=3.8.0", "dask=2021.04.0", "distributed=2021.04.0", "prefect", "seaborn", "boto3"]},
#     pip={''}
#     )

executor = DaskExecutor(
    cluster_class=coiled.Cluster,
    cluster_kwargs={
        "software": "kvnkho/prefect",
        "shutdown_on_close": True,
        "name": "prefect-cluster",
    },
)
flow.executor = executor

flow.register("omlds")