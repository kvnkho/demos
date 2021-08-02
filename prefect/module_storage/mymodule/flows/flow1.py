from prefect import Flow, task
from prefect.storage import Module

@task
def abc(x):
    return x

with Flow("module_test") as flow:
    abc(1)

flow.storage=Module("mymodule.flows.flow1")