from prefect import Flow, task
import prefect
from prefect.storage.git import Git

@task
def abc():
    return 1

with Flow("gh storage") as flow:
    abc()

flow.storage = Git(
repo="kvnkho/demos", 
flow_path="prefect/git_storage.py",
repo_host="github.com")

flow.register("git_storage")