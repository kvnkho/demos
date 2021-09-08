from prefect import task, Flow
import time
from prefect.run_configs import KubernetesRun

@task
def abc(x):
    time.sleep(5)
    return x

with Flow("timer flow") as flow:
    abc.map([1]*10)

flow.storage = Github
flow.run_config = KubernetesRun()
flow.register("dsdc")