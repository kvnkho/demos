import prefect
from prefect import task, Flow
from prefect.run_configs import LocalRun
from prefect.executors import DaskExecutor
from prefect.storage import Github

# RUN_CONFIG = ECSRun(labels=["simple-ecs"], execution_role_arn="arn:aws:iam::304383062342:role/ecsTaskExecutionRole")
RUN_CONFIG = LocalRun()

EXECUTOR = DaskExecutor(cluster_class="dask_cloudprovider.aws.FargateCluster",
                        cluster_kwargs={'n_workers': 1, 'region_name':'us-east-2'})
STORAGE = Github(repo="kvnkho/demos", flow_path='prefect/fargate_test.py', repo_host="github.com")

@task
def print_log():
  logger = prefect.context.get("logger")
  logger.info("I am running")
  
with Flow(name="ecs-test", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as flow:
  print_log()

flow.register(project_name="aws")