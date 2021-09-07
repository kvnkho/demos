import prefect
from prefect import Flow, task
from prefect.run_configs import DockerRun
from prefect.storage import Local

from components.componentA import ComponentA 
from components.componentB import ComponentB

@task
def test_task():
    logger = prefect.context.get("logger")
    x = ComponentA(2)
    y = ComponentB(2)
    x = x.n + y.n
    logger.info(f"Test {x}!")  # Should return 4
    return

with Flow("docker_example", 
          storage=Local(path="/app/workflow/flow.py",stored_as_script=True), 
          run_config=DockerRun(image="test:latest")) as flow:
    test_task()