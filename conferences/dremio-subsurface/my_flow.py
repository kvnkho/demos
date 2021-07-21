from dremio_connection import connect_to_dremio_flight_server_endpoint

import prefect
from prefect import Flow, task, case
import great_expectations as ge
from prefect.tasks.notifications import SlackTask

query = """
    SELECT vendor_id, pickup_date, dropoff_date,
       passenger_count FROM "Demo NYC Taxi"."Demo Taxi Reflection" LIMIT 10000
"""

@task
def fetch_data(query):
    df = connect_to_dremio_flight_server_endpoint(hostname="3.238.152.255", 
                                                  flightport=32010, 
                                                  username="kvnkho",
                                                  password="Prefect123", 
                                                  sqlquery=query, tls=None, certs=None)
    return df

@task
def validate_data(df):
    df = ge.from_pandas(df)
    df.expect_column_values_to_be_between("passenger_count", min_value=1, max_value=12, mostly=0.99, result_format="SUMMARY")
    df.expect_column_values_to_be_in_set("vendor_id", ['VTS', 'CMT', 'DDS', 'TS', 'MT', 'DS'])
    results = df.validate()
    return results

@task
def examine_results(validation):
    if validation["success"] == True:
        return True
    return False


with Flow("validation") as flow:
    df = fetch_data(query)
    results = validate_data(df)
    examine = examine_results(results)
    with case(examine, False):
        SlackTask(webhook_secret = "SLACK_WEBHOOK_URL")(message = task(lambda x: "The validation failed \n" + str(x))(results))

# flow.run()
flow.register("dremio")