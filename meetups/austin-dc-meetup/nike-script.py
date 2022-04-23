from prefect import Flow, task
from prefect.engine.results import S3Result
from prefect.storage import S3 
from prefect.executors import LocalDaskExecutor

@task(result=S3Result(bucket="coiled-prefect"))
def mytask():
    return "abc"

with Flow("flow_name", result=S3Result(bucket="coiled-prefect"), 
            storage=S3(bucket="coiled-prefect"), 
            executor=LocalDaskExecutor()) as flow:
    mytask()

print(flow.serialize())

OrderedDict([('name', 'flow_name'), ('type', 'prefect.core.flow.Flow'), ('schedule', None), ('parameters', []), ('tasks', [{'name': 'mytask', 'retry_delay': None, 'outputs': 'typing.Any', 'max_retries': 0, 'auto_generated': False, 'slug': 'mytask-1', 'cache_for': None, 'tags': [], 'inputs': {}, 'trigger': {'fn': 'prefect.triggers.all_successful', 'kwargs': {}}, 'skip_on_upstream_skip': True, 'type': 'prefect.tasks.core.function.FunctionTask', 'timeout': None, 'cache_key': None, 'cache_validator': {'fn': 'prefect.engine.cache_validators.never_use', 'kwargs': {}}, '__version__': '1.2.0'}]), ('edges', []), ('reference_tasks', []), ('run_config', None), ('__version__', '1.2.0'), ('storage', {'stored_as_script': False, 'secrets': [], 'key': None, 'client_options': None, 'bucket': 'coiled-prefect', 'flows': {}, '__version__': '1.2.0', 'type': 'S3'})])