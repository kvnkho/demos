from prefect import Flow, task, unmapped
from typing import Any
from prefect.executors import LocalDaskExecutor
from pycaret.datasets import get_data
import pandas as pd
from prefect.backend.artifacts import create_markdown_artifact
from sklearn.model_selection import train_test_split

from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.svm import SVC
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

@task(nout=4)
def create_data():
    df = get_data("titanic")
    df = df.drop(["Name", "PassengerId", "Ticket", "Cabin"], axis = 1)
    df["Sex"] = pd.factorize(df["Sex"])[0]
    dummy = pd.get_dummies(df['Embarked'], prefix='Cabin')
    df = pd.concat([df.drop("Embarked", axis=1), dummy], axis = 1)
    y = df["Survived"]
    X = df.drop("Survived", axis = 1)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    fill_age = X_train["Age"].mean()
    X_train["Age"] = X_train["Age"].fillna(fill_age)
    X_test["Age"] = X_test["Age"].fillna(fill_age)
    return X_train, X_test, y_train, y_test

@task
def get_models():
    return [LogisticRegression(random_state=42),
            KNeighborsClassifier(), DecisionTreeClassifier(), SVC(), 
            RandomForestClassifier(n_estimators=200, max_depth=4, random_state=42),
            RandomForestClassifier(n_estimators=100, max_depth=3, random_state=42)]

@task
def train_model(model: Any, X_train, X_test, y_train, y_test):
    clf = model.fit(X_train, y_train)
    y_pred = clf.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    return {"model": model.__class__.__name__, "params": model.get_params(), "accuracy": acc}

@task
def get_results(results):
    res = pd.DataFrame(results)
    create_markdown_artifact(res.to_markdown())
    return res

with Flow("distributed") as flow:
    X_train, X_test, y_train, y_test = create_data()
    models = get_models()
    training_runs = train_model.map(models, unmapped(X_train), unmapped(X_test), unmapped(y_train), unmapped(y_test))
    get_results(training_runs)

flow.executor = LocalDaskExecutor()
flow.register("bristech")
