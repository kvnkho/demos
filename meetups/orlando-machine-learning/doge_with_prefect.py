import requests as re
import pandas as pd
from datetime import datetime, timedelta
import coiled
from prefect.tasks.control_flow.case import case

from prefect import task, Flow, Parameter, unmapped
from prefect.tasks.notifications import SlackTask
from prefect.executors import DaskExecutor
from prefect.run_configs.kubernetes import KubernetesRun


def format_url(coin="DOGE"):
    url = "https://production.api.coindesk.com/v2/price/values/"
    start_time = (datetime.now() - timedelta(minutes=10)).isoformat(timespec="minutes")
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

post_to_slack = SlackTask(message="There has been a dip in crypto price.", webhook_secret="SLACK_WEBHOOK_URL")

with Flow("to-the-moon") as flow:
    coin = Parameter("coin", default=["DOGE", "BTC", "ETH"])
    threshold = Parameter("threshold", default=0)
    data = get_data.map(coin)
    is_dip = detect_dip.map(data, threshold=unmapped(threshold))
    dip = task(lambda x: max(x))(is_dip)
    with case(dip, True):
        post_to_slack()

# # Create a software environment for our workers
# coiled.create_software_environment(
#     name="prefect",
#     conda={"channels": ["conda-forge"],
#             "dependencies": ["python=3.8.0", "dask=2021.04.0", "distributed=2021.04.0", "prefect"]},
#     )

# executor = DaskExecutor(
#     cluster_class=coiled.Cluster,
#     cluster_kwargs={
#         "software": "kvnkho/prefect",
#         "shutdown_on_close": True,
#         "name": "prefect-cluster",
#     },
# )
# flow.executor = executor
flow.register("budapest")