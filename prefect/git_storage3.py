from prefect import Flow, task
import time
from prefect.run_configs import KubernetesRun
from prefect.engine.state import State, Cancelled
import prefect
from prefect.storage import GitHub

def post_to_slack_handler_task(obj, old_state: State, new_state: State) -> State:
    if isinstance(new_state, Cancelled):
        prefect.context.logger.info("HIT THE CANCELLED STATE HANDLER FROM TASK")
    return new_state

def post_to_slack_handler_flow(obj, old_state: State, new_state: State) -> State:
    if isinstance(new_state, Cancelled):
        prefect.context.logger.info("HIT THE CANCELLED STATE HANDLER FROM FLOW")
    return new_state

@task(state_handlers=[post_to_slack_handler_task])
def abc(x):
    time.sleep(10)
    return 1

with Flow("sleep", state_handlers=[post_to_slack_handler_flow]) as flow:
    abc.map([1,2,3])

storage = GitHub(repo="kvnkho/demos", 
            path="/prefect/git_storage3.py",
            ref="main")

flow.storage = storage
flow.run_config = KubernetesRun(image="prefecthq/prefect:latest-python3.8")
flow.register("databricks")
