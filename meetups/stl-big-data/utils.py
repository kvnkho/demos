import pandas as pd
import numpy as np

def make_data(**groups):
    """
    cat: given by groups
    f1: uniform 0-1
    f2: uniform 3-4
    f3: f1<f3<f2
    f4: normal mu=0, std=1
    f5: always starts with k
    f6: different normal distributions
    f7: rand choice specified by each group
    f8: always starts with category name
    """
    n = sum(x["count"] for x in groups.values())
    p = [x["count"]/n for x in groups.values()]
    np.random.seed(0)
    data = dict(
        cat = np.random.choice(list(groups.keys()),n,p=p),
        feature1 = np.random.rand(n),
        feature2 = np.random.rand(n)+3,
        feature3 = np.random.rand(n),
        feature4 = np.random.normal(0,1,n),
    )
    df = pd.DataFrame(data)
    df["feature3"] = df["feature3"] + df["feature1"]
    df["feature5"] = np.random.choice(["kdfgdjj","kgpo","koiov"],n)
    
    def gen(pdf):
        gp = pdf["cat"].iloc[0]
        params = groups[gp]
        pdf["feature6"] = np.random.normal(params["mu"],params["std"],pdf.shape[0])
        pdf["feature7"] = np.random.choice(params["c"],pdf.shape[0])
        return pdf

    df = df.groupby("cat").apply(gen)
    df["feature8"] = df["cat"].str.cat(np.random.choice(["ndjj","uopopo","xcxxv"],n))    
    return df

def update_values(series, n, func):
    for p in np.random.choice(range(len(series)), n):
        series.iat[p] = func(series.iat[p])
        
def make_noise(df, ratio, **groups):
    df = df.copy()
    np.random.seed(0)
    s = int(df.shape[0]*ratio)
    update_values(df["feature1"], s, lambda x:x+1)
    update_values(df["feature1"], s, lambda x:x-1)
    update_values(df["feature2"], s, lambda x:x-2)
    update_values(df["feature2"], s, lambda x:float("nan"))
    update_values(df["feature3"], s, lambda x:x+5)
    df["feature4"] = np.random.normal(0.5,1.5,df.shape[0])
    update_values(df["feature5"], s, lambda x:"p"+x)
    update_values(df["feature8"], s, lambda x:"p"+x)
    
    def gen(pdf):
        gp = pdf["cat"].iloc[0]
        if gp not in groups:
            return pdf
        params = groups[gp]
        pdf["feature6"] = np.random.normal(params["mu"],params["std"],pdf.shape[0])
        pdf["feature7"] = np.random.choice(params["c"],pdf.shape[0])
        return pdf
    
    df = df.groupby("cat").apply(gen)
    
    return df

def setup():
    baseline = make_data(
        a=dict(count=50000, mu=0.5, std=0.5, c=["c1","c2"]),
        b=dict(count=50000, mu=4.5, std=0.5, c=["c2","c3"])
    )
    # baseline.to_parquet(os.path.join(root,"data","baseline.parquet"))
    df1 = make_noise(
        baseline, 0.0001,
        b=dict(mu=0.5, std=1, c=["c1","c2","c3"])
    )
    # df1.to_parquet(os.path.join(root,"data","new_data.parquet"))
    return baseline, df1

