from prefect import Flow, task, Parameter
import prefect
from prefect.schedules import Schedule
from prefect.schedules import clocks, Schedule
from datetime import timedelta
from prefect.run_configs import DockerRun
from prefect.storage import GitHub
import time

@task
def some_task(x):
    time.sleep(5)
    prefect.context.logger.info(x)
    return x

with Flow("reregistration_test") as flow:
    some_task("testing")
    some_task("testing")
    some_task("testing")
    some_task("testing")
    some_task("testing")
    some_task("testing")

flow.register("databricks")

storage = GitHub(repo="kvnkho/demos", 
            path="/prefect/git_storage5.py",
            ref="main")

flow.storage = storage
flow.run_config = DockerRun()
flow.register("databricks")
