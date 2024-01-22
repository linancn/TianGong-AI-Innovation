import ast
import os
import time

import pandas as pd
from sqlalchemy import create_engine, text


def extract_number(s):
    lst = ast.literal_eval(s)
    return lst[1]


start_time = time.time()

# df = pd.read_csv("data/concatenated.csv", header=None, nrows=2)
df = pd.read_csv("data/concatenated.csv", header=None)

df["uuid"] = df[0].apply(lambda x: x.split("_")[0])

for index, row in df.iterrows():
    for col in df.columns[1:33]:
        df.at[index, col] = extract_number(row[col])

df[df.columns[1:33]] = df[df.columns[1:33]].astype("float32")

numeric_cols = df.select_dtypes(include=[float]).columns

grouped_df = df.groupby("uuid")[numeric_cols].mean().reset_index()

# grouped_df_t = grouped_df.transpose()
grouped_df["Sum"] = grouped_df.iloc[:, 1:33].sum(axis=1)
grouped_df["Mean"] = grouped_df.iloc[:, 1:33].mean(axis=1)
grouped_df["Median"] = grouped_df.iloc[:, 1:33].median(axis=1)
grouped_df["Std"] = grouped_df.iloc[:, 1:33].std(axis=1)
grouped_df["Min"] = grouped_df.iloc[:, 1:33].min(axis=1)
grouped_df["Max"] = grouped_df.iloc[:, 1:33].max(axis=1)
grouped_df["Range"] = grouped_df["Max"] - grouped_df["Min"]
grouped_df["Variance"] = grouped_df.iloc[:, 1:33].var(axis=1)
grouped_df["Skewness"] = grouped_df.iloc[:, 1:33].skew(axis=1)
grouped_df["Kurtosis"] = grouped_df.iloc[:, 1:33].kurt(axis=1)
grouped_df["Quantile_25"] = grouped_df.iloc[:, 1:33].quantile(q=0.25, axis=1)
grouped_df["Quantile_50"] = grouped_df.iloc[:, 1:33].quantile(q=0.5, axis=1)
grouped_df["Quantile_75"] = grouped_df.iloc[:, 1:33].quantile(q=0.75, axis=1)
grouped_df["Quantile_90"] = grouped_df.iloc[:, 1:33].quantile(q=0.9, axis=1)

grouped_df.to_excel("data/all.xlsx")

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
