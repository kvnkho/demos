import prefect
from prefect import task, Flow
from prefect.run_configs import ECSRun
from prefect.s
@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello World!")
    return 0
task_definition = {
    'networkMode': 'awsvpc',
    'cpu': 1024,
    'memory': 2048,
    'containerDefinitions': [
        {'name': 'flow'}
    ]
}
run_config = ECSRun(
    image='prefecthq/prefect',
    task_definition=task_definition
)
flow = Flow("hello-flow",
            tasks=[hello_task],
            run_config=run_config)
flow.register(project_name='Prefect_Tutorials', labels=['aws', 'ecs', 'fargate'])