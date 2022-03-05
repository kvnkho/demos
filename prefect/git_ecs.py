import prefect
from prefect.storage import GitHub
from prefect.run_configs import ECSRun
from prefect import task, Flow

RUN_CONFIG = ECSRun(
    task_definition=dict(
        family="prefect-test",
        requiresCompatibilities=["FARGATE"],
        networkMode="awsvpc",
        cpu=1024,
        memory=2048,
        taskRoleArn="arn:aws:iam::029864677731:role/prefect-ecs",
        executionRoleArn="arn:aws:iam::029864677731:role/prefect-ecs",
        containerDefinitions=[
            {
                "name":"flow",
                "image":"prefecthq/prefect",
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": "prefect-flow",
                        "awslogs-region": "us-west-2",
                        "awslogs-create-group": "true",
                        "awslogs-stream-prefix": "ecs"
                    }
                }
            }
        ],
    ),
    run_task_kwargs=dict(cluster="test-cluster"),
)

@task
def say_hi():
    logger = prefect.context.get("logger")
    logger.info("Hi from Prefect from flow")
    return

with Flow("logging_test_ecs", run_config=RUN_CONFIG,) as flow:
    say_hi()

flow.storage = GitHub(
                repo="kvnkho/demos", 
                path="/prefect/git_ecs.py",
                ref="main")
flow.register("general_assembly")
