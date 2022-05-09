from prefect import task, Flow
from prefect.run_configs import ECSRun, DockerRun
from prefect.storage import S3
from prefect.executors import LocalDaskExecutor
import prefect

@task(log_stdout=True)
def abc(x):
    prefect.context.logger.info(x)
    print(x)
    return "hello"

with Flow("ecs_test", run_config=DockerRun(), storage=S3(bucket="coiled-prefect", add_default_labels=False, secrets=["AWS_CREDENTIALS"]),
           executor = LocalDaskExecutor(scheduler="processes")) as flow:
    abc.map(list(range(10)))

flow.register("databricks")
