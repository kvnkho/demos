import prefect
from prefect import Flow, task
from prefect.client.secrets import Secret
from prefect.storage import GitHub
from prefect.run_configs import ECSRun


STORAGE = 

RUN_CONFIG = ECSRun(
    labels=["prod"],
    task_definition_path="s3://prefectdata/flow_task_definition.yaml",
    run_task_kwargs=dict(cluster="prefectEcsCluster"),
)


@task(log_stdout=True)
def hello_world():
    text = f"hello from {FLOW_NAME} from Prefect version {prefect.__version__}"
    print(text)
    return text


with Flow(FLOW_NAME, storage=STORAGE, run_config=RUN_CONFIG,) as flow:
    hw = hello_world()