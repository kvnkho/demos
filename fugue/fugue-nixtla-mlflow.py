# This is a script how to run Nixtla and Fugue on top of MLFlow
# Let's pretend you have 100 distinct timeseries. You want to create
# a model for each one.

# Focus on the logic for one and how to write that first.
# Wrap it in a function and then use Fugue to partition the DataFrame
# into 100 partitions where each partition is a timeseries.

# We use Spark to run the function on each timeseries.
# For each run, we save the artifacts and metrics.

# We will run this on local setting, but a similar setup should
# work on Databricks + S3.

# Generate 100 distinct timeseries
from statsforecast.utils import generate_series
import pandas as pd

def create_data(n = 100) -> pd.DataFrame:
    series = generate_series(n_series=n, seed=1).reset_index()
    series['unique_id'] = series['unique_id'].astype(int)
    return series

# Create function to model one time series
from statsforecast.models import AutoARIMA
from statsforecast.core import StatsForecast

def run_model(df: pd.DataFrame, horizon=7) -> pd.DataFrame:
    """
    The logic here can be flexible. I will return the 
    trained model and predictions.
    """
    # Trim the horizon out for metrics calculations
    df = df.iloc[:-horizon]
    model = StatsForecast(df=df,
                      models=[AutoARIMA()], 
                      freq='D',
                      )
    preds = model.fit_predict(horizon)
    return model, preds

# MLFlow logging function
import mlflow
import os
import random
import pickle

def run_experiment(df: pd.DataFrame, horizon=7) -> pd.DataFrame:
    """
    This function will get sent to workers. I am setting the stuff here 
    for consistency across workers. This will log the experiment for
    one time series.

    When the df enters this function, it will already be partitioned such
    that it is one timeseries.
    """
    unique_id = df.iloc[0]['unique_id']

    mlflow.set_experiment("nixtla-spark")
    # Make sure the MLFlow server is up
    mlflow.set_tracking_uri("http://localhost:5000")

    with mlflow.start_run() as r:
        mlflow.log_param('horizon', horizon)

        model, preds = run_model(df, horizon)

        # The metric will require cross validation to calculate
        # I am just simplifying things by using a random number
        # But you can calculate for each timeseries.
        
        mlflow.log_metric('rmse', random.random())

        # Save model and artifacts
        if not os.path.exists("outputs"):
            os.makedirs("outputs")
        
        # Preds for timeseries
        preds_filename = f"outputs/{unique_id}-preds.csv"
        preds.to_csv(preds_filename)
        model_filename = f"outputs/{unique_id}-model.pkl"
        
        with open(model_filename, 'wb') as handle:
            pickle.dump(model, handle)
        
        mlflow.log_artifact(preds_filename)
        mlflow.log_artifact(model_filename)
    
    # Just returning the preds
    return preds.reset_index()

# # We can now already call the mlflow function on smaller data
# # to test:
# series = create_data(1)
# run_experiment(series, horizon=7)

# Bring to Spark. Because we did everything for one timeseries,
# we can now bring things Spark by using Fugue.

from fugue import transform

series = create_data(100)
res = transform(series, 
                run_experiment, 
                schema="unique_id:int, ds:date, AutoARIMA:float",
                partition={"by": "unique_id"},
                params={"horizon": 7},
                engine="spark")
res.show()