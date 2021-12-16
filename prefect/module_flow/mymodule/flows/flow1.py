from prefect import Flow, task
import prefect
from prefect.storage import Module

@task
def abc(x):
    prefect.context.logger.info(x)
    return x

with Flow("module_test") as flow1:
    abc(1)
