from prefect import Flow, task
import prefect
from prefect.run_configs.kubernetes import KubernetesRun
from prefect.storage.github import GitHub

@task
def abc():
    logger = prefect.context.get("logger")
    logger.info(prefect.context.config.logging)
    return "a"

with Flow("logger-test") as flow:
    abc()

RUN_CONFIG = KubernetesRun(env=({"PREFECT__LOGGING__EXTRA__LOGGERS": "['boto3', 'something']"}))
flow.run_config = RUN_CONFIG
flow.storage = GitHub(repo="kvnkho/demos", path="prefect/kubernetes_loggers.py")

flow.register("dsdc")