import pandas as pd

# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None
product = None

# -
df = pd.DataFrame({"col1": [1,2,3], "col2":[2,3,4]})
df.to_parquet(product["data"])
