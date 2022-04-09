import pandas as pd
import pandera as pa
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score

from prefect import task, flow
from prefect.task_runners import DaskTaskRunner

from utils import setup
from typing import Any

@task
def load_data():
    baseline, new_data = setup()
    baseline.drop(["feature5", "feature8"], axis=1, inplace=True)
    new_data.drop(["feature5", "feature8"], axis=1, inplace=True)
    baseline["feature7"] = pd.factorize(baseline["feature7"])[0]
    new_data["feature7"] = pd.factorize(new_data["feature7"])[0]
    baseline.to_parquet("baseline.parquet", index=False)
    new_data.to_parquet("new_data.parquet", index=False)
    return baseline

@task
def validate_data(df):
    schema = pa.DataFrameSchema(
        columns={
            "feature1": pa.Column(float, checks=[pa.Check.gt(0), pa.Check.lt(1)], nullable=False),
            "feature2": pa.Column(float, checks=[pa.Check.gt(3), pa.Check.lt(4)], nullable=False),
        },
        # define checks at the DataFrameSchema-level
        checks=pa.Check(
            lambda df: (df["feature3"] > df["feature1"]) & (df['feature3'] < df['feature2']),
            name="feature3_between"
        )
    )
    return schema.validate(df, lazy=True)

@task
def get_models(config: str = "all"):
    from tune import Space, Grid, RandInt, Rand

    space1 = Space(model=LogisticRegression, solver="lbfgs", C=Grid(10,20), penalty=Grid("l2","none"))
    space2 = Space(model=RandomForestClassifier, max_samples=Rand(0.8,1), max_depth=RandInt(3,4)).sample(4)
    if config == "lr":
        space = space1
    if config == "rfc":
        space = space2
    if config == "all":
        space = space1 + space2
    space = [x.simple_value for x in list(space)]
    models = []
    for model_params in space:
        model = model_params.pop("model")
        models.append(model(**model_params))
    return models

@task
def train_model(model: Any, df: pd.DataFrame):
    y = df["cat"]
    X = df.drop("cat", axis = 1)
    X_train, X_test, y_train, y_test = train_test_split(X, y, 
                                                        test_size=0.2, 
                                                        random_state=42)
    clf = model.fit(X_train, y_train)
    y_pred = clf.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    return {"model_object": model, 
            "model": model.__class__.__name__, 
            "params": model.get_params(), 
            "accuracy": acc}

@task
def get_best_model(outputs):
    accs = [x["accuracy"] for x in outputs]
    index_max = np.argmax(accs)
    model = outputs[index_max]["model_object"]
    return model

@task
def save_best_model(model):
    from joblib import dump
    dump(model, "model.pkl") 
    return

@flow(name="Training Flow", task_runner=DaskTaskRunner(
    cluster_kwargs={"n_workers": 4}
))
def training_flow(config: str = "all"):
    df = load_data()
    df = validate_data(df)
    
    models = get_models(config)
    out = []
    for model in models.wait().result():
        out.append(train_model(model, df))
    model = get_best_model(out)
    save_best_model(model)

if __name__ == "__main__":
    training_flow(config = "rfc")