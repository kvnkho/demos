from prefect import Flow, task, Parameter
from prefect.storage.git import Git

@task
def test(x):
    return x + 1

with Flow("trevor", storage=Git(repo="kvnkho/demos", flow_path='trevor.py', repo_host="github.com")) as flow:
    x = Parameter('x', default=2)
    test(x)
