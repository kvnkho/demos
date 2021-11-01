from prefect import Flow, task
from prefect.executors import DaskExecutor
import coiled

@task
def do_nothing(n):
    pass

listy = list(range(200000))

with Flow("map_testing") as flow:
    
    do_nothing.map(listy)

# coiled.create_software_environment(
#     name="prefect",
#     conda={
#         "channels": ["conda-forge", "defaults", "fastchan"],
#         "dependencies": [
#             "python==3.8.0",
#             "prefect",
#             "bokeh",
#         ]
#     })

executor = DaskExecutor(
    cluster_class=coiled.Cluster,
    cluster_kwargs={
        "software": "kvnkho/prefect",
        "shutdown_on_close": True,
        "name": "prefect-cluster",
    },
)
flow.executor = executor
# flow.register("dsdc")

flow.run()