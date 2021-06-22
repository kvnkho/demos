from prefect import Flow, task
import prefect
from prefect.storage.github import Github

@task
def abc():
    return 1

with Flow("gh storage") as flow:
    abc()

flow.storage = Github(
repo="kvnkho/demos", 
flow_path="git_storage.py",)

flow.register("git_storage")
