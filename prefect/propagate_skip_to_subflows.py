import prefect
from prefect import Flow, task, Parameter
from prefect.engine.state import Skipped
from prefect.tasks.prefect import StartFlowRun
from prefect.engine import signals
from prefect.client.client import Client
from prefect.tasks.control_flow.case import case
import datetime
@task
def check_bucket():
    # Logic to check for new files
    data = []
    if len(data) == 0:
        raise signals.SKIP()
    else:
        pass
    return data
@task
def register_data(data):
    if len(data) > 1:
        for point in data:
            logger = prefect.context.get("logger")
            logger.info(point)
        return
@task
def preprocess_data():
    x = 1
    logger = prefect.context.get("logger")
    logger.info(f"Executed {x}")
    return x
@task
def check_previous_skip(flow_run_signal):
    client = Client()
    flow_state = client.get_flow_run_info(flow_run_signal.state.message.split(' ')[0])
    state = [t for t in flow_state.task_runs if "check_bucket" in t.task_slug][0].state
    if state.is_skipped():
        logger = prefect.context.get("logger")
        logger.info("There was a skip in previous Flow")
        return True
    else:
        return False
with Flow("flow_1") as flow1:
    data = check_bucket()
    register_data(data)
with Flow("flow_2") as flow2:
    preprocess_data()
# Register block
flow1.register("testing-result")
flow2.register("testing-result")
start = StartFlowRun(project_name="testing-result", wait = True)
with Flow('master-flow') as flow:
    run_id = start(flow_name="flow_1")
    # True or False
    skipped = check_previous_skip(run_id)
    # Insert other flows here
    with case(skipped, False):
        start(flow_name="flow_2")
        start(flow_name="flow_2")
flow.run()