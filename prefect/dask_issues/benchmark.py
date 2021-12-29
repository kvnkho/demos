from prefect import Flow, task
from prefect.executors import DaskExecutor
import dask_kubernetes

@task
def do_nothing(n):
    pass

listy = list(range(200000))

with Flow("map_testing") as flow:
    
    do_nothing.map(listy)

executor=DaskExecutor(
    cluster_class="dask_kubernetes.KubeCluster",
    cluster_kwargs={
        "pod_template": make_pod_spec(
            memory_request="1Gi",
            memory_limit="2Gi",
            image="gcr.io/radiant-labs-pipeline-v5/prefect-base:3.8",
            threads_per_worker=2,
        ),
    },
    adapt_kwargs=dict(minimum=2, maximum=2),
    debug=True,
)
flow.executor = executor
flow.run_config = KubernetesRun()
flow.storage = GitHub(repo="kvnkho/demos", path="prefect/dask_issues/benchmark.py")

flow.register("dask_issue")
