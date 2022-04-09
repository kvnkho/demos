from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule
from datetime import timedelta

DeploymentSpec(
    name="to-the-moon-deployment",
    flow_location="/Users/kevinkho/Work/demos/meetups/stl-big-data/doge-with-prefect.py",
    tags=['tutorial','test'],
    parameters={"coins":["DOGE","BTC","ETH"]},
)