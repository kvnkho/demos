from prefect import Flow, task, Parameter
import prefect
from prefect.schedules import Schedule
from prefect.schedules import clocks, Schedule
from datetime import timedelta
from prefect.run_configs import DockerRun
from prefect.storage import GitHub

@task
def some_task(x):
    import pandas as pd
    prefect.context.logger.info(x)
    return x

with Flow("schedule_test", run_config=DockerRun(image="test:latest")) as flow:
    some_task("testing")

flow.register("databricks")


storage = GitHub(repo="kvnkho/demos", 
            path="/prefect/git_storage4.py",
            ref="main")

flow.storage = storage
flow.run_config = DockerRun(image="test:latest")
flow.register("databricks")
