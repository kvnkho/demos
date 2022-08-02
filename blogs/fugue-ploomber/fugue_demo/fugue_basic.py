import pandas as pd
from fugue import transform 
from sklearn.preprocessing import minmax_scale

df = pd.DataFrame({"col1": ["A","A","A","B","B","B"], "col2":[1,2,3,4,5,6]})

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(scaled=minmax_scale(df["col2"]))

# run on Pandas
pdf = transform(df.copy(), normalize, schema="*,scaled:float", partition={"by":"col1"})
# run on Dask
ddf = transform(df.copy(), normalize, schema="*,scaled:float", partition={"by":"col1"} ,engine="dask")