from prefect import Flow, task
from prefect.storage import Module
import prefect

@task
def abc(x):
    prefect.context.logger.info(x)
    return x

with Flow("module_test2") as flow2:
    abc(2)
