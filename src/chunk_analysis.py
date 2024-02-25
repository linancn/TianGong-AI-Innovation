import time
from itertools import groupby
from multiprocessing import Pool

import numpy as np
import pandas as pd

uuids_list = pd.read_csv("data/metadata_review.csv",usecols=range(1))
uuids = uuids_list["uuid"]
uuids = set(uuids)


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


def uuid_split(data):
    split_data = data[0].str.split("_", expand=True)
    data["uuid"] = split_data[0]
    data["chunkid"] = split_data[1]
    return data


def arccos(data):
    cols = data.columns[1:13]
    data[cols] = np.clip(data[cols], -1, 1)
    data[cols] = np.degrees(np.arccos(data[cols]))
    return data

def review_or_not(data):
    data['review_or_not'] = np.where(data['uuid'].isin(uuids), 'Y', data['review_or_not'])
    return data

def convert_to_int(data):
    data['chunkid'] = data['chunkid'].astype(int)
    return data

def sort_df(data):
    data = data.sort_values(['uuid', 'chunkid'])
    return data

def compute_mean(data):
    data["mean"] = data.iloc[:, 1:13].mean(axis=1)
    return data

def compute_departe(data):
    data["departe"] = data["mean"] - chunk_overall_mean
    return data

start_time = time.time()

# df = pd.read_csv("data/output.csv", header=None, usecols=range(13))
df = pd.read_csv("data/output.csv", header=None, usecols=range(13), nrows=2000)
df['review_or_not'] = np.nan
print("Data loaded")

n_cores = 16

df = parallelize_dataframe(df, uuid_split, n_cores)
print("id column created")

df = parallelize_dataframe(df, arccos, n_cores)
print("arccos calculated")

df = parallelize_dataframe(df, review_or_not, n_cores)
print("review_or_not calculated")

df = parallelize_dataframe(df, convert_to_int, n_cores)
print("chunkid converted to int")

df=parallelize_dataframe(df, sort_df, n_cores)
print("Data sorted")

df = parallelize_dataframe(df, compute_mean, n_cores)
print("Mean calculated")

#calculate the mean of all chunks
chunk_overall_mean = df["mean"].mean()
print(chunk_overall_mean)

df= parallelize_dataframe(df, compute_departe, n_cores)
print("Departe computed")

print(df.head(3))

df.iloc[:, -5:].to_csv("12_class_sort.csv")
print("Data saved")

end_time = time.time()
print("Time taken: ", end_time - start_time)

