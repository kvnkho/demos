from fugue import transform
import pandas as pd

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["extract"]
product = None
engine = None

# -
df = pd.read_parquet(upstream["extract"]["data"])

def add_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["col3"] = df["col1"] + df["col2"]
    return df

add_cols(df)

df.to_parquet(product["data"])