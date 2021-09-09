from prefect import task, Flow, Parameter
import time
from prefect.storage.github import GitHub
import logging
import prefect

# Creating a filter
class SecureFilter(logging.Filter):
    def filter(self, rec):
        if 'sec' in rec.msg:
            return 0
        return 1

@task
def abc(x):
    time.sleep(x)
    return x

logger = logging.getLogger("prefect.TaskRunner")
logger.addFilter(SecureFilter())

with Flow("timer flow") as flow:
    secs= Parameter("secs", 1)
    abc.map([secs]*5)

flow.storage = GitHub(
repo="kvnkho/demos", 
path="/prefect/logging_filter.py",
ref="main")

flow.register("dsdc")
# flow.run()