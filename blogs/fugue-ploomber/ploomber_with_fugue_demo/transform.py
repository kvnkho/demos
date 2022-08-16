import pandas as pd
from fugue import transform 
from sklearn.preprocessing import minmax_scale

# %% tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["extract"]
product = None
engine = None

# %%
def normalize(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(scaled=minmax_scale(df["col2"]))

transform(upstream["extract"]["data"], 
          normalize, 
          schema="*,scaled:float",
          partition={"by":"col1"},
          engine=engine,
          save_path=product["data"])


ddf = dd.from_pandas(df, npartitions=2)
ddf = transform(ddf, engine="dask")
ddf = transform(ddf2, engine="dask")