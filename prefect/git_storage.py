from prefect import Flow, task
import prefect
from prefect.storage.github import GitHub

@task
def abc():
    return 1

with Flow("gh storage") as flow:
    abc()

flow.storage = GitHub(
repo="kvnkho/demos", 
path="git_storage.py",)

flow.register("omlds")
