from prefect import Flow, task
from prefect.storage import GitHub
from prefect.run_configs import DockerRun
from prefect.storage.git import Git
import os
from pathlib import Path

FLOW_NAME = "docker_script"

storage = GitHub(repo="kvnkho/demos", 
                path="/prefect/git_storage3.py",
                ref="main")

@task(log_stdout=True)
def create_directory():
    text = f"..."
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    local_path = os.path.join(ROOT_DIR, "data")

    Path(local_path).mkdir(parents=True, exist_ok=True)
    return text

with Flow(FLOW_NAME,storage=storage,run_config=DockerRun()) as flow:
    create_dir = create_directory()

flow.register(project_name="databricks")
