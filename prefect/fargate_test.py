import prefect
from prefect import task, Flow
from prefect.run_configs import LocalRun
from prefect.executors import DaskExecutor
from prefect.storage.github import GitHub

RUN_CONFIG = LocalRun(env= {'PREFECT__LOGGING__LEVEL': 'DEBUG'})

EXECUTOR = DaskExecutor(cluster_class="dask_cloudprovider.aws.FargateCluster",
                        cluster_kwargs={'image':'prefecthq/prefect:0.14.17-python3.7','n_workers': 1, 'region_name':'us-east-2'})
STORAGE = GitHub(repo="kvnkho/demos", path='prefect/fargate_test.py')

@task
def print_log2():
  logger = prefect.context.get("logger")
  logger.info("I am running")
  
with Flow(name="ecs-test", storage=STORAGE, run_config=RUN_CONFIG, executor=EXECUTOR) as flow:
  print_log2()

flow.register(project_name="aws")