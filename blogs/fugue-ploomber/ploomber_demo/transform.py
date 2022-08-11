import pandas as pd
from sklearn.preprocessing import minmax_scale

# %% tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["extract"]
product = None
engine = None

# %%
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(scaled=minmax_scale(df["col2"]))

df = pd.read_parquet(upstream["extract"]["data"])
df = df.groupby("col1").apply(lambda x: normalize(x))
df.to_parquet(product["data"])