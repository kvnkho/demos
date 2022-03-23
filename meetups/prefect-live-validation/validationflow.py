from prefect import flow, task, get_run_logger

import pandas as pd
import pandera as pa

schema = pa.DataFrameSchema(
    columns={
        "feature1": pa.Column(float, checks=[pa.Check.gt(0), pa.Check.lt(1)], nullable=False),
        "feature5": pa.Column(str, pa.Check.str_startswith("k"), coerce=True),
        "feature10": pa.Column(str, pa.Check.str_startswith("k"), required=False)
    }
)

@task
def get_data():
    return pd.read_parquet("new_data.parquet")

@task
def validate_dataframe(df: pd.DataFrame):
    df = schema.validate(df)
    return df

@task
def error_reporting():
    logger = get_run_logger()
    logger.info("THIS FLOW FAILED")
    return

@flow
def validation_flow():
    df = get_data()
    val = validate_dataframe(df)
    if isinstance(val.wait().result(), pa.errors.SchemaErrors):
        error_reporting()

validation_flow()