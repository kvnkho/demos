from prefect import task, Flow, Parameter
from prefect.tasks.prefect import StartFlowRun
from prefect.storage import GitHub

with Flow("token-test") as flow:
    StartFlowRun(project_name="testing", flow_name="flow_must_fail")()

flow.storage = GitHub(repo="kvnkho/demos", path="/prefect/token_test.py")
flow.register("testing")