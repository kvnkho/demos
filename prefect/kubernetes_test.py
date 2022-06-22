from prefect import Flow, task 
from prefect.run_configs import KubernetesRun
from prefect.storage import GitHub

@task
def abc():
    return 1

with Flow("k8s") as flow:
    abc()

flow.storage = GitHub(
                repo="kvnkho/demos", 
                path="/prefect/kubernetes_test.py",
                ref="main")
flow.run_config = KubernetesRun()
flow.register("databricks")
