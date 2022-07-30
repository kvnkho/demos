import pandas as pd

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["extract"]
product = None

# -
df = pd.read_csv(upstream["extract"]["data"])

def add_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["col3"] = df["col1"] + df["col2"]
    return df

add_cols(df)
df.to_csv(product["data"], index=False)