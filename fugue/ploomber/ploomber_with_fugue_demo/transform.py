import pandas as pd
from fugue import transform 

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = ["extract"]
product = None
engine = None

# -

def add_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["col3"] = df["col1"] + df["col2"]
    return df

df = pd.read_csv(upstream["extract"]["data"])
df = transform(df, add_cols, schema="*,col3:int", engine=engine)

if engine == "spark":
    df.write.option("header",True).mode('overwrite').csv(product["data"])
else:
    df.to_csv(product["data"], index=False)