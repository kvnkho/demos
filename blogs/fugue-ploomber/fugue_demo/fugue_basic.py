import pandas as pd
from fugue import transform 

df = pd.DataFrame({"col1": [1,2,3], "col2":[2,3,4]})

def add_cols(df: pd.DataFrame) -> pd.DataFrame:
    df["col3"] = df["col1"] + df["col2"]
    return df

ddf = transform(df, add_cols, schema="*,col3:int", engine="dask")