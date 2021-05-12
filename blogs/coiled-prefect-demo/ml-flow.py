import dask.dataframe as dd
from distributed import wait
from dask_ml.model_selection import train_test_split
# from dask_ml.linear_model import LinearRegression
# from dask_ml.metrics import mean_absolute_error
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error

from prefect import Flow, task
import prefect
import joblib

@task(name="Load Data")
def load_data():
    df = dd.read_csv(
        "s3://nyc-tlc/trip data/yellow_tripdata_2019-01.csv",
        dtype={
            "payment_type": "UInt8",
            "VendorID": "UInt8",
            "passenger_count": "UInt8",
            "RatecodeID": "UInt8",
        },
        storage_options={"anon": True}
    )
    df = df[['payment_type', 'fare_amount', 'trip_distance', 'passenger_count', 'tpep_pickup_datetime', 'tip_amount']]
    return df

@task(name="Create Features")
def create_features(df: dd.DataFrame) -> dd.DataFrame:
    # Create hour of day feature
    df['hour'] = dd.to_datetime(df["tpep_pickup_datetime"],unit='ns').dt.hour
    df = df.drop("tpep_pickup_datetime", axis=1)
    return df

@task(name="Train Model")
def train_model(df: dd.DataFrame):
    # This brings the DataFrame from Dask to Pandas
    df = df.compute()

    train, test = train_test_split(df, test_size = 0.2)

    est = LinearRegression()
    est.fit(train.drop('tip_amount', axis=1), train['tip_amount'])

    y_test = test['tip_amount']
    y_pred = est.predict(test.drop('tip_amount', axis = 1))
    metric = mean_absolute_error(y_test, y_pred)

    logger = prefect.context.get("logger")
    logger.info(f"The mean absolute error is: {metric}")

    return est

@task(name="Save Model")
def save_model(est):
    joblib.dumps(est, "/tmp/model.pkl")
    return

with Flow("ml-flow") as flow:
    df = load_data()
    df = create_features(df)
    train_model(df)

flow.run()
flow.exectuor = LocalDaskExecutor()