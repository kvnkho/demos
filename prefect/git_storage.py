from prefect import Flow, task
import prefect
from prefect.storage.github import GitHub
from prefect.run_configs import DockerRun
from prefect.engine.signals import PAUSE

@task
def abc():
    raise PAUSE()
    return 1

with Flow("tes") as flow:
    abc()

flow.storage = GitHub(
repo="kvnkho/demos", 
path="/prefect/git_storage.py",
ref="main")

flow.run_config = DockerRun(image="prefecthq/prefect:latest")
flow.register("databricks")
