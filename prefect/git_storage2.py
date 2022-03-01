from prefect import Flow, task
import prefect
from prefect.storage.github import GitHub
from prefect.run_configs import DockerRun

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

flow.run_config = DockerRun(image="myprefectregistry.azurecr.io/samples/prefect:latest")
flow.register("general_assembly")
