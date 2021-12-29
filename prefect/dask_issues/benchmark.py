from prefect import Flow, task
from prefect.executors import DaskExecutor
import dask_kubernetes
from prefect.storage import GitHub
from prefect.run_configs import KubernetesRun

@task
def do_nothing(n):
    pass

listy = list(range(200000))

with Flow("map_testing") as flow:
    
    do_nothing.map(listy)

executor=DaskExecutor(
    cluster_class="dask_kubernetes.KubeCluster",
    cluster_kwargs={
        "n_workers": 2
    },
    debug=True,
)
flow.executor = executor
flow.run_config = KubernetesRun(env={"EXTRA_PIP_PACKAGES": "dask_kubernetes"})
flow.storage = GitHub(repo="kvnkho/demos", path="prefect/dask_issues/benchmark.py")

flow.register("dask_issue")
