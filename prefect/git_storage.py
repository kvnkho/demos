from prefect import Flow, task
import prefect
from prefect.storage.github import GitHub
from prefect.run_configs import DockerRun

@task
def abc():
    return 1

with Flow("test-docker") as flow:
    abc()

flow.storage = GitHub(
repo="kvnkho/demos", 
path="/prefect/git_storage.py",
ref="main")

flow.run_config = DockerRun(image="prefecthq/prefect")

flow.register("omlds")
