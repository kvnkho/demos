import os
os.environ['PREFECT__EXTRA__LOGGERS'] = "['mymodule']"
os.environ['PREFECT__LOGGING__LEVEL'] = "DEBUG"

import modA.mymodule
from prefect import Flow, task

@task
def thetask():
    modA.mymodule.myfunction()
    modA.mymodule.otherfunction()
    return

with Flow("log_test") as flow:
    thetask()

flow.run()
