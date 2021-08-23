from prefect import task, Flow
from prefect.run_configs import ECSRun
from prefect.storage import S3

@task
def abc():
    return "hello"

with Flow("ecs_test") as flow:
    abc()

RUN_CONFIG = ECSRun(task_definition_path="test.yaml", 
                            labels=['ecs_test_c','ecs_test_d'],
                            run_task_kwargs={'cluster': 'test-cluster'},
                            task_role_arn= 'arn:aws:iam::12345678:role/prefect-ecs',
                            execution_role_arn='arn:aws:iam::12345678:role/prefect-ecs',
                            image= 'prefecthq/prefect:latest-python3.8'
                            )

flow.storage=S3(bucket="coiled-prefect", add_default_labels=False)

flow.run_config = RUN_CONFIG 

flow.register("dsdc")

"""
prefect agent ecs start --cluster arn:aws:ecs:us-east-2:12345678:cluster/test-cluster --label ecs_test_c --label ecs_test_d
"""