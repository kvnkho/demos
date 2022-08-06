import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from typing import Iterable, List, Any, Dict
from fugue import transform

spark = SparkSession.builder.getOrCreate()

design = ["old", "current", "new"]
colors = ["red", "blue", "green", "yellow"]
size = ["S", "M", "L", "XL"]

# treat this dataframe like shirts
data = pd.DataFrame({"color": np.random.choice(colors, 10000),
                     "size": np.random.choice(size, 10000),
                     "design": np.random.choice(design, 10000),
                     "price": np.random.randint(50,100),
                     "units": np.random.randint(100,500)})

class MyLogic:

    def __init__(self, data, engine): 
        self.data = data
        self.engine = engine
        
    def preprocess1(self):
        # this will operate on whole dataframe
        def rename_design(df: pd.DataFrame) -> pd.DataFrame:
            # rename all 'current' to 'old'
            df.loc[df["design"] == "current", "design"] = "old"
            return df
        self.data = transform(self.data, rename_design, schema="*", engine=self.engine)
        return self
        
    def parallelize1(self):
        # increase shirt price based on color
        price_increase = {"red": 10, "blue": 0, "green": 5, "yellow": 10}
        def increase_price(df: pd.DataFrame, increases:Dict) -> pd.DataFrame:
            # this is already partitioned
            color = df.iloc[0]['color']
            df = df.assign(price=df['price'] + increases[color])
            return df
        self.data = transform(self.data, 
                              increase_price, 
                              schema="*", 
                              partition={"by": "color"},
                              params={"increases": price_increase},
                              engine=self.engine)
        return self

    def postprocess1(self):
        # this will operate on whole dataframe
        def calculate_value(df: pd.DataFrame) -> pd.DataFrame:
            return df.assign(value=df["price"]*df["units"])
        self.data = transform(self.data, calculate_value, schema="*,value:int", engine=self.engine)
        return self

# Test on Pandas
print(MyLogic(data, engine=None).preprocess1().parallelize1().postprocess1().data.head())

# Bring to Spark (change head to show)
sdf = spark.createDataFrame(data)
MyLogic(sdf, engine=spark).preprocess1().parallelize1().postprocess1().data.show()