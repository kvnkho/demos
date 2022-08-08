import pandas as pd
from fugue import transform
import pickle

test = pd.DataFrame({"a": ["A","A","B","B"], "b": [1,2,3,4]})

from typing import List, Iterable, Dict, Any

def output_two(df: pd.DataFrame) -> Iterable[Dict[str,Any]]:
    metrics = pd.DataFrame({"accuracy": [0.5], "precision": [0.2], "recall": [0.3]})
    preds = pd.DataFrame({"date": [1,2,3], "pred": [2,3,4]})

    yield {"metrics": pickle.dumps(metrics), "predictions": pickle.dumps(preds)}

# Model for each group
res = transform(test, output_two, schema="metrics:binary,predictions:binary", partition={"by": "a"})
print(res.head())

# This is for one group
metrics = pickle.loads(res.iloc[0]["metrics"])
print(metrics.head())
