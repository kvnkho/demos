import prefect
from prefect.storage import GitHub
from prefect.run_configs import ECSRun
from prefect import task, Flow, Parameter


STORAGE = GitHub(
repo="kvnkho/demos", 
path="/prefect/ecs_flow_memory.py",
ref="main")

RUN_CONFIG = ECSRun(
    cpu="1 vcpu",  
    memory="8 GB",
    task_definition_path="s3://coiled-prefect/flow_task_definition.yaml",
    run_task_kwargs=dict(cluster="test-cluster"),
)

@task(log_stdout=True)
def hello_world(x):
    text = f"hello from Prefect version {prefect.__version__}"
    print(text)
    return x


with Flow("ecs-memory-test", storage=STORAGE, run_config=RUN_CONFIG,) as flow:
    x = Parameter("x test", 3)
    hw = hello_world(x)

flow.register("general_assembly")