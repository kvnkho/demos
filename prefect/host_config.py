from prefect import Flow, task
from prefect.run_configs.docker import DockerRun
from prefect.storage import GitHub

@task
def abc():
    import time
    time.sleep(60)
    return 1

with Flow("host_config") as flow:
    abc()

run_config = DockerRun(host_config=dict(mem_limit=1234567,
                                        mem_reservation=123456))
flow.run_config = run_config

flow.storage = GitHub(
repo="kvnkho/demos", 
path="/prefect/host_config.py",
ref="main")

flow.register("bristech")
