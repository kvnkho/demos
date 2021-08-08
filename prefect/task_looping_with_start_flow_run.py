from prefect import Parameter, Flow, task, Task
import prefect
from prefect.tasks.prefect import StartFlowRun
from time import sleep
from prefect.engine.signals import LOOP, SUCCESS
import datetime

class DoSomething(Task):
    def run(self, value):
        sleep(1)
        self.logger.info(value)
do_something = DoSomething()

with Flow("sub-flow") as sub_flow:
    one_date = Parameter("one_date")
    do_something(one_date)
sub_flow.register("aws")

sub_flow_task = StartFlowRun(project_name="aws", flow_name="sub-flow", wait=True)

@task()
def loop_over_dates(dates):
    # Starting state
    loop_payload = prefect.context.get("task_loop_result", {"dates": dates})
    dates = loop_payload.get("dates", [])
    logger = prefect.context.get("logger")
    logger.info(dates)
    one_date = dates[0]
    logger.info(f"Checking {one_date}")
    try:
        sub_flow_task.run(parameters={"one_date": one_date}, 
        idempotency_key=datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))
    except SUCCESS:
        # Don't exit the loop on Flow Run success
        pass
    # Drop the first date
    dates.pop(0)
    if len(dates) == 0:
        return  # return statements end the loop
    raise LOOP(message=f"Processing {dates[0]}", result=dict(dates = dates))

with Flow("schedule flow") as schedule_flow:
    date_param = Parameter("dates", default=[1,2,3,4,5])
    loop_over_dates(date_param)
    
schedule_flow.register("aws")