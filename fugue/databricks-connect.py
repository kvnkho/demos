from fugue import FugueWorkflow
from fugue_spark import SparkExecutionEngine

import pandas as pd

from typing import Iterable, Any, Dict

df = pd.DataFrame({'numbers':[1,2,3,4], 'words':['hello','world','apple','banana']})

# schema: *, reversed:str
def reverse_word(df: pd.DataFrame) -> pd.DataFrame:
    df['reversed'] = df['words'].apply(lambda x: x[::-1])
    return df

with FugueWorkflow(SparkExecutionEngine) as dag:
    df = dag.df(df)
    df = df.transform(reverse_word)
    df.show()