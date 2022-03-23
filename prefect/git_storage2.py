from prefect import Flow, task
import prefect
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage.github import GitHub

@task
def something():
    prefect.context.logger.info("hello")
    return 1

with Flow("name") as flow:
    something()

flow.storage = GitHub(
                repo="kvnkho/demos", 
                path="/prefect/git_storage2.py",
                ref="main")

flow.run_config = KubernetesRun(labels=["test"])
flow.register("databricks")
