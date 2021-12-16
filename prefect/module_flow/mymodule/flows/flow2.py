from prefect import Flow, task
from prefect.storage import Module

@task
def abc(x):
    prefect.context.logger.info(x)
    return x

with Flow("module_test2") as flow:
    abc(2)

flow.storage=Module("mymodule.flows.flow2")