import os

import pandas as pd
from sqlalchemy import create_engine, text

df = pd.read_csv("data/result.csv", header=None)

df["uuid"] = df[0].apply(lambda x: x.split("_")[0])

numeric_cols = df.select_dtypes(include=[float]).columns

grouped_df = df.groupby("uuid")[numeric_cols].mean().reset_index()

# grouped_df_t = grouped_df.transpose()
grouped_df["Sum"] = df.iloc[:, 1:32].sum(axis=1)
grouped_df["Mean"] = df.iloc[:, 1:32].mean(axis=1)
grouped_df["Median"] = df.iloc[:, 1:32].median(axis=1)
grouped_df["Std"] = df.iloc[:, 1:32].std(axis=1)
grouped_df["Min"] = df.iloc[:, 1:32].min(axis=1)
grouped_df["Max"] = df.iloc[:, 1:32].max(axis=1)
grouped_df["Range"] = grouped_df["Max"] - grouped_df["Min"]
grouped_df["Variance"] = df.iloc[:, 1:32].var(axis=1)
grouped_df["Skewness"] = df.iloc[:, 1:32].skew(axis=1)
grouped_df["Kurtosis"] = df.iloc[:, 1:32].kurt(axis=1)
grouped_df["Quantile_25"] = df.iloc[:, 1:32].quantile(q=0.25, axis=1)
grouped_df["Quantile_50"] = df.iloc[:, 1:32].quantile(q=0.5, axis=1)
grouped_df["Quantile_75"] = df.iloc[:, 1:32].quantile(q=0.75, axis=1)
grouped_df["Quantile_90"] = df.iloc[:, 1:32].quantile(q=0.9, axis=1)


def create_engine_pg():
    engine = create_engine(
        os.environ["DATABASE_URL"],
    )
    return engine


def select_from_db(_engine, query):
    with _engine.begin() as conn:
        df = pd.read_sql_query(sql=text(query), con=conn)
    return df


query = "SELECT uuid, doi, title, journal, authors, date FROM journals WHERE journal = 'RESOURCES, CONSERVATION AND RECYCLING' AND embedding_time IS NOT NULL AND authors IS NOT NULL"

engine = create_engine_pg()

df_db = select_from_db(engine, query)

df_db["uuid"] = df_db["uuid"].astype(str)
grouped_df["uuid"] = grouped_df["uuid"].astype(str)

merged_df = df_db.merge(grouped_df, on="uuid", how="inner")

merged_df.to_excel("data/final.xlsx")
