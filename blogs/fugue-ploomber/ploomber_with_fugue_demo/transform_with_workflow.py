from fugue import FugueWorkflow
import pandas as pd

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["extract"]
product = None
engine = None

# -
def add_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["col3"] = df["col1"] + df["col2"]
    return df

with FugueWorkflow(engine) as dag:
    df = dag.load(upstream["extract"]["data"])
    df = df.transform(add_cols, schema="*, col3:int")
    df.save(product["data"], mode="overwrite", single=True)