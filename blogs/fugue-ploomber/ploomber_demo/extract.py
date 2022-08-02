import pandas as pd

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None
product = None

# -
df = pd.DataFrame({"col1": ["A","A","A","B","B","B"], "col2":[1,2,3,4,5,6]})
df.to_parquet(product["data"])
