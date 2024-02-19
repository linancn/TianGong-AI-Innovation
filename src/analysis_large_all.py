import ast
import time
from multiprocessing import Pool

import pandas as pd


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


def extract_number(s):
    lst = ast.literal_eval(s)
    return lst[1]


def uuid_split(data):
    data["uuid"] = data[0].apply(lambda x: x.split("_")[0])
    return data


def process_data(data):
    for index, row in data.iterrows():
        for col in data.columns[1:33]:
            try:
                data.at[index, col] = extract_number(row[col])
            except:
                data.at[index, col] = -100
    return data


def compute_sum(data):
    data["Sum"] = data.iloc[:, 1:33].sum(axis=1)
    return data


def compute_mean(data):
    data["Mean"] = data.iloc[:, 1:33].mean(axis=1)
    return data


def compute_median(data):
    data["Median"] = data.iloc[:, 1:33].median(axis=1)
    return data


def compute_std(data):
    data["Std"] = data.iloc[:, 1:33].std(axis=1)
    return data


def compute_min(data):
    data["Min"] = data.iloc[:, 1:33].min(axis=1)
    return data


def compute_max(data):
    data["Max"] = data.iloc[:, 1:33].max(axis=1)
    return data


def compute_range(data):
    data["Range"] = data["Max"] - data["Min"]
    return data


def compute_variance(data):
    data["Variance"] = data.iloc[:, 1:33].var(axis=1)
    return data


def compute_skewness(data):
    data["Skewness"] = data.iloc[:, 1:33].skew(axis=1)
    return data


def compute_kurtosis(data):
    data["Kurtosis"] = data.iloc[:, 1:33].kurt(axis=1)
    return data


def compute_quantile_25(data):
    data["Quantile_25"] = data.iloc[:, 1:33].quantile(q=0.25, axis=1)
    return data


def compute_quantile_50(data):
    data["Quantile_50"] = data.iloc[:, 1:33].quantile(q=0.5, axis=1)
    return data


def compute_quantile_75(data):
    data["Quantile_75"] = data.iloc[:, 1:33].quantile(q=0.75, axis=1)
    return data


def compute_quantile_90(data):
    data["Quantile_90"] = data.iloc[:, 1:33].quantile(q=0.9, axis=1)
    return data


start_time = time.time()

df = pd.read_csv("data/output.csv", header=None)
# df = pd.read_csv("data/output.csv", header=None, nrows=20000)
print("Data loaded")

# n_cores = mp.cpu_count()
n_cores = 16

df = parallelize_dataframe(df, uuid_split, n_cores)
print("uuid column created")

df = parallelize_dataframe(df, process_data, n_cores)
print("Data processed")

df[df.columns[1:33]] = df[df.columns[1:33]].astype("float32")
print("Data type changed")

numeric_cols = df.select_dtypes(include=[float]).columns
print("Numeric columns selected")

grouped_df = df.groupby("uuid")[numeric_cols].mean().reset_index()
print("Data grouped")

# grouped_df_t = grouped_df.transpose()
# grouped_df["Sum"] = grouped_df.iloc[:, 1:33].sum(axis=1)
# grouped_df["Mean"] = grouped_df.iloc[:, 1:33].mean(axis=1)
# grouped_df["Median"] = grouped_df.iloc[:, 1:33].median(axis=1)
# grouped_df["Std"] = grouped_df.iloc[:, 1:33].std(axis=1)
# grouped_df["Min"] = grouped_df.iloc[:, 1:33].min(axis=1)
# grouped_df["Max"] = grouped_df.iloc[:, 1:33].max(axis=1)
# grouped_df["Range"] = grouped_df["Max"] - grouped_df["Min"]
# grouped_df["Variance"] = grouped_df.iloc[:, 1:33].var(axis=1)
# grouped_df["Skewness"] = grouped_df.iloc[:, 1:33].skew(axis=1)
# grouped_df["Kurtosis"] = grouped_df.iloc[:, 1:33].kurt(axis=1)
# grouped_df["Quantile_25"] = grouped_df.iloc[:, 1:33].quantile(q=0.25, axis=1)
# grouped_df["Quantile_50"] = grouped_df.iloc[:, 1:33].quantile(q=0.5, axis=1)
# grouped_df["Quantile_75"] = grouped_df.iloc[:, 1:33].quantile(q=0.75, axis=1)
# grouped_df["Quantile_90"] = grouped_df.iloc[:, 1:33].quantile(q=0.9, axis=1)

grouped_df = parallelize_dataframe(grouped_df, compute_sum, n_cores)
print("Sum computed")
grouped_df = parallelize_dataframe(grouped_df, compute_mean, n_cores)
print("Mean computed")
grouped_df = parallelize_dataframe(grouped_df, compute_median, n_cores)
print("Median computed")
grouped_df = parallelize_dataframe(grouped_df, compute_std, n_cores)
print("Std computed")
grouped_df = parallelize_dataframe(grouped_df, compute_min, n_cores)
print("Min computed")
grouped_df = parallelize_dataframe(grouped_df, compute_max, n_cores)
print("Max computed")
grouped_df = parallelize_dataframe(grouped_df, compute_range, n_cores)
print("Range computed")
grouped_df = parallelize_dataframe(grouped_df, compute_variance, n_cores)
print("Variance computed")
grouped_df = parallelize_dataframe(grouped_df, compute_skewness, n_cores)
print("Skewness computed")
grouped_df = parallelize_dataframe(grouped_df, compute_kurtosis, n_cores)
print("Kurtosis computed")
grouped_df = parallelize_dataframe(grouped_df, compute_quantile_25, n_cores)
print("Quantile_25 computed")
grouped_df = parallelize_dataframe(grouped_df, compute_quantile_50, n_cores)
print("Quantile_50 computed")
grouped_df = parallelize_dataframe(grouped_df, compute_quantile_75, n_cores)
print("Quantile_75 computed")
grouped_df = parallelize_dataframe(grouped_df, compute_quantile_90, n_cores)
print("Quantile_90 computed")


grouped_df.to_excel("data/all_large.xlsx")

end_time = time.time()

print(f"Execution time: {end_time - start_time} seconds")

# def create_engine_pg():
#     engine = create_engine(
#         os.environ["DATABASE_URL"],
#     )
#     return engine


# def select_from_db(_engine, query):
#     with _engine.begin() as conn:
#         df = pd.read_sql_query(sql=text(query), con=conn)
#     return df


# query = "SELECT uuid, doi, title, journal, authors, date FROM journals WHERE journal = 'RESOURCES, CONSERVATION AND RECYCLING' AND embedding_time IS NOT NULL AND authors IS NOT NULL"

# engine = create_engine_pg()

# df_db = select_from_db(engine, query)

# df_db["uuid"] = df_db["uuid"].astype(str)
# grouped_df["uuid"] = grouped_df["uuid"].astype(str)

# merged_df = df_db.merge(grouped_df, on="uuid", how="inner")

# merged_df.to_excel("data/final.xlsx")
