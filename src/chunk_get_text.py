import os

import pandas as pd
from dotenv import load_dotenv
from pinecone import Pinecone

load_dotenv()

pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
index = pc.Index(os.environ["PINECONE_INDEX"])

df = pd.read_csv("data/id-chunk-samples.csv")
df["merged_id"] = df.apply(lambda row: f"{row['id']}_{row['chunk']}", axis=1)

ids_list = df["merged_id"].tolist()
fetch_response = index.fetch(ids=ids_list)
df["text"] = df["merged_id"].apply(
    lambda id: (
        fetch_response["vectors"][id]["metadata"]["text"]
        if id in fetch_response["vectors"]
        else ""
    )
)
df.to_csv('text_text.csv', index=False)
print(df)
