import prefect
import random

from prefect import task, Flow, Parameter
from prefect.storage.s3 import S3

from typing import Iterable


@task
def extract(count: int) -> Iterable[int]:
    fake_data = range(10)
    return random.sample(fake_data, k=count)


@task
def transform(records: Iterable[int]) -> Iterable[int]:
    return [n + 1 for n in records]


@task
def load(data: Iterable[int]) -> bool:
    logger = prefect.context.get("logger")
    logger.info(f"Pretending to store {data}")
    return True

with Flow("increment a random sample") as f:
    records = extract(Parameter("count", default=100))
    data = transform(records)
    load(data)

storage = S3(
    bucket="coiled-prefect",
    key="prefect/flows/demo/random-sample-wlb2",
    stored_as_script=True,
    local_script_path="./s3_test.py",
)

f.storage = storage

storage.add_flow(f)
storage.build()


# f.run(count=100, executor=executor)