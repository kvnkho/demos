import prefect
from prefect import Flow, task
from prefect.run_configs import DockerRun
from prefect.storage import Local

from components.componentA import ComponentA 
from components.componentB import ComponentB
from prefect.storage import Docker

@task
def test_task():
    logger = prefect.context.get("logger")
    x = ComponentA(2)
    y = ComponentB(2)
    x = x.n + y.n
    logger.info(f"Test {x}!")  # Should return 4
    return

FLOW_STORAGE = Docker(
                image_name='pm-prefect',
                image_tag='dev',
                dockerfile="/Users/kevinkho/Work/demos/blogs/prefect-docker/docker_with_local_storage/Dockerfile"
)

with Flow("docker_example", 
          storage=FLOW_STORAGE, 
          run_config=DockerRun(image="test:latest")) as flow:
    test_task()

FLOW_STORAGE.build()