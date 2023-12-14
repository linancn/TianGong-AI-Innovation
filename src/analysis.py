import pandas as pd

df = pd.read_csv("data/result.csv", header=None)

df["UUID"] = df[0].apply(lambda x: x.split("_")[0])

numeric_cols = df.select_dtypes(include=[float]).columns

grouped_df = df.groupby("UUID")[numeric_cols].mean().reset_index()

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



grouped_df.to_csv("data/final.csv")


# df_remove_head_tail = df.drop(df.columns[[1, -1]], axis=1)


# print(df_remove_head_tail)
