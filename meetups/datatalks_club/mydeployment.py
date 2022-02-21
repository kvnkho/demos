from datetime import timedelta
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule
from datetime import timedelta

DeploymentSpec(
    flow_location="/Users/kevinkho/Work/demos/meetups/datatalks_club/doge.py",
    name="to-the-moon",
    schedule=IntervalSchedule(interval=timedelta(minutes=1)),
    tags=["earth"],
)