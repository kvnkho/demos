from fugue import FugueWorkflow
from fugue_spark import SparkExecutionEngine

from prefect import task, Flow, Parameter
import pandas as pd

from typing import Iterable, Any, Dict

def reverse_word(df: pd.DataFrame) -> pd.DataFrame:
    df['reversed'] = df['words'].apply(lambda x: x[::-1])
    return df


@task
def create_data():
    return pd.DataFrame({'numbers':[1,2,3,4], 'words':['hello','world','apple','banana']})

@task
def print_head(df):
    print(df)

@task
def run_prefect(df: Any, engine: Any) -> None:
    with FugueWorkflow(engine) as dag:
        df = dag.df(df)
        df = df.transform(reverse_word, schema="*, reversed:str")
        df.show()
    return df.result.native

with Flow('dag') as flow:
    engine = Parameter('engine', default=None)
    df = create_data()
    df = run_prefect(df, engine)
    print_head(df)

# flow.run()
flow.run(parameters={'engine': SparkExecutionEngine})