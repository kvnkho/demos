from prefect import task, Flow
from prefect.run_configs import ECSRun, DockerRun
from prefect.storage import S3, GitHub
from prefect.executors import LocalDaskExecutor
import prefect

@task(log_stdout=True)
def abc(x):
    prefect.context.logger.info(x)
    print(x)
    return "hello"

with Flow("ecs_test", run_config=DockerRun(), executor = LocalDaskExecutor(scheduler="threads")) as flow:
    abc(1)

flow.storage = GitHub(
repo="kvnkho/demos", 
path="/prefect/ecs/ecs_log.py",
ref="main")
flow.register("databricks")
