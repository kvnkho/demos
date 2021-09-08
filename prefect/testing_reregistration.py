from prefect import task, Flow
import time
from prefect.run_configs import KubernetesRun
from prefect.storage.github import GitHub

@task
def abc(x):
    time.sleep(5)
    return x

with Flow("timer flow") as flow:
    abc.map([1]*10)

flow.storage = GitHub(
repo="kvnkho/demos", 
path="/prefect/testing_reregistration.py",
ref="main")
flow.run_config = KubernetesRun()
flow.register("dsdc")