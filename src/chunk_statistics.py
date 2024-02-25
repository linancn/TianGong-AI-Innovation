from multiprocessing import Pool

import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv("data/12_class_sort.csv")
#calculate the mean of all chunks
chunk_overall_mean = df["Mean"].mean()
print(chunk_overall_mean)

def parallelize_dataframe(df, func, n_cores):
    pool = Pool(n_cores)
    size = len(df) // n_cores
    dfs = [df.iloc[i * size : (i + 1) * size] for i in range(n_cores)]
    if len(df) % n_cores != 0:
        dfs[-1] = pd.concat([dfs[-1], df.iloc[n_cores * size :]])
    df = pd.concat(pool.map(func, dfs))
    pool.close()
    pool.join()
    return df

def compute_departe(data):
    chunk_overall_mean = data["Mean"].mean()
    data["departe"] = data["Mean"] - chunk_overall_mean
    return data

df= parallelize_dataframe(df, compute_departe, 16)
print("Departe computed")

print(df.head(3))

df.to_csv("data/departe.csv", index=False)


