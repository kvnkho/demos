import os
os.environ["SLACK_WEBHOOK_URL"] = "https://hooks.slack.com/services/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"

import pandas as pd
import numpy as np
import requests as re
import json

from prefect import task, flow

from typing import Any

from evidently.model_profile import Profile
from evidently import ColumnMapping
from evidently.model_profile.sections import DataDriftProfileSection, CatTargetDriftProfileSection 

@task
def load_data():
    baseline = pd.read_parquet("baseline.parquet")
    new_data = pd.read_parquet("new_data.parquet")
    return baseline, new_data

@task
def check_drift(baseline, new_data):
    target = 'cat'

    numerical_features = ['feature1','feature2','feature3','feature4','feature6']
    categorical_features = ['feature7']
    column_mapping = ColumnMapping(target,
                                'prediction',
                                numerical_features=numerical_features,
                                categorical_features=categorical_features)
    data_drift_profile = Profile(sections=[DataDriftProfileSection(), CatTargetDriftProfileSection()])
    data_drift_profile.calculate(baseline, new_data, column_mapping=column_mapping)
    data_drift_profile = json.loads(data_drift_profile.json())
    return data_drift_profile

@task
def load_best_model():
    from joblib import load
    model = load("model.pkl")
    return model

@task
def send_to_slack(message: str):
    r = re.post(
        os.environ["SLACK_WEBHOOK_URL"],
        json=message if isinstance(message, dict) else {"text": message},
    )
    r.raise_for_status()
    return

@flow(name="Prediction Flow")
def prediction_flow(threshold_drift: int = 2):
    baseline, new_data = load_data().wait().result()
    metric = check_drift(baseline, new_data).wait().result()
    if metric["data_drift"]["data"]["metrics"]["n_drifted_features"] > threshold_drift:
        send_to_slack("FEATURE DRIFT DETECTED")
    else:
        model = load_best_model().wait().result()
        new_data["preds"] = model.predict(new_data.drop("cat", axis=1).fillna(1))
        new_data.to_parquet("preds.parquet")

if __name__ == "__main__":
    prediction_flow(threshold_drift = 4)