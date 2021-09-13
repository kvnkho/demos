import prefect
from prefect import task, Flow
from prefect.run_configs import DockerRun
from prefect.storage import GitHub


@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello world!")


with Flow(
        "hello-flow",
        storage=GitHub(add_default_labels=False),
        run_config=DockerRun(
            image="prefecthq/prefect:latest",
            labels=["docker"],
        )
) as flow:
    hello = hello_task()


if __name__ == '__main__':
    flow.register(project_name='Test')
