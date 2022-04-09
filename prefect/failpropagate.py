# from prefect import task, Flow
# import prefect
# import datetime
# from prefect.triggers import manual_only

# @task
# def task_one():
#     prefect.context.logger.info(datetime.datetime.now())
#     return 1

# @task(trigger=manual_only)
# def task_two(x):
#     prefect.context.logger.info(datetime.datetime.now())
#     return x+1

# @task
# def task_three(x):
#     prefect.context.logger.info(datetime.datetime.now())
#     return x+1

# with Flow("fail_propagate") as flow:
#     (task_three(task_two(task_one()))

# flow.register("general_assembly")

from prefect.tasks.secrets import PrefectSecret

print(PrefectSecret("SLACK_WEBHOOK_URL").run())