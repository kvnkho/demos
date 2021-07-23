import coiled
from prefect import task, Flow
import torch
import prefect
from prefect.executors import DaskExecutor
from prefect.storage import GitHub

@task
def abc(x):
    logger = prefect.context.get("logger")
    logger.info(torch.cuda.is_available())
    logger.info(torch.cuda.device_count())
    logger.info(f"Map index {x}")
    return x

with Flow("gpu-test") as flow:
    abc.map([1,2,3,4])

coiled.create_software_environment(
    name="gpu-env4",
    container="gpuci/miniconda-cuda:10.2-runtime-ubuntu18.04",
    conda={
        "channels": ["conda-forge", "defaults", "fastchan"],
        "dependencies": [
            "python==3.8.5",
            "pytorch",
            "torchvision",
            "cudatoolkit=10.2",
            "prefect", 
            "fastai",
            "scikit-image",
            "numpy",
            "dask",
            "bokeh>=0.13.0",
            "dask-cuda",
            "prefect",
            "pandas"
        ]
    })

executor = DaskExecutor(
    cluster_class=coiled.Cluster,
    cluster_kwargs={
        "software": "kvnkho/gpu-env4",
        "n_workers": 2,
        "shutdown_on_close": True,
        "name": "prefect-executor",
        "worker_memory": "4 GiB",
        "worker_gpu": 1,
        "backend_options":{"spot": False}
    },
)

flow.executor = executor
flow.storage = GitHub(repo="kvnkho/demos", path="/prefect/coiled_gpu.py", ref="main")
flow.register("dremio")