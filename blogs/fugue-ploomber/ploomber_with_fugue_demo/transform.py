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

df = transform(upstream["extract"]["data"], 
               add_cols, 
               schema="*,col3:int",
               engine=engine,
               save_path=product["data"],)