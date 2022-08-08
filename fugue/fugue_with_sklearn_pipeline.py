import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from typing import Iterable, List, Any, Dict
from fugue import transform

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline

class DropColumn(BaseEstimator, TransformerMixin):
    def __init__(self, column):
        self.column = column
        
    def fit(self, X, y = None):
        return self

    def transform(self, X, y = None):
        return X.drop(self.column, axis=1)

class IncreasePrice(BaseEstimator, TransformerMixin):
    def __init__(self, increases:Dict):
        self.increases = increases
        self.color = None
        
    def fit(self, X, y = None):
        # assumes this is already partitioned by color
        self.color = X.iloc[0]["color"]
        return self

    def transform(self, X, y = None):
        return X.assign(price=X['price'] + self.increases[self.color])


design = ["old", "current", "new"]
colors = ["red", "blue", "green", "yellow"]
size = ["S", "M", "L", "XL"]

# treat this dataframe like shirts
data = pd.DataFrame({"color": np.random.choice(colors, 10000),
                     "size": np.random.choice(size, 10000),
                     "design": np.random.choice(design, 10000),
                     "price": np.random.randint(50,100, 10000),
                     "units": np.random.randint(100,500, 10000)})

price_increase = {"red": 10, "blue": 0, "green": 5, "yellow": 10}
pipeline = Pipeline([('increase_price', IncreasePrice(price_increase)), 
                      ('drop', DropColumn('design')), 
                      ('pass',None)], 
                      verbose = True)
 

def apply_pipeline(df: pd.DataFrame, pipeline: Pipeline) -> pd.DataFrame:
    # Guess units based on price design
    y = df['units']
    X = df.drop('units', axis=1)
    pipeline.fit(X, y)
    res = pipeline.transform(X)
    res = res.assign(y=y)
    return res

# Fugue portion
# This will partition the pipeline by color and run the transformation
# on each section and then combine them together
# The assumption is that nothing in the Pipeline relies on global aggregations
# such as global median or max. Every partition should be independent

# Run on Pandas
out = transform(data, 
                apply_pipeline, 
                schema="*-design-units+y:int",
                params={"pipeline": pipeline},
                partition={"by": "color"})
print(out.head())

# Run on Spark
out = transform(data, 
                apply_pipeline, 
                schema="*-design-units+y:int",
                params={"pipeline": pipeline},
                partition={"by": "color"},
                engine="spark")
out.show()