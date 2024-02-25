import csv
import os

import pandas as pd
from pinecone import Pinecone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv()

pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
index = pc.Index(os.environ["PINECONE_INDEX"])


def create_engine_pg():
    engine = create_engine(
        os.environ["DATABASE_URL"],
    )
    return engine


def select_from_db(_engine, query):
    with _engine.begin() as conn:
        df = pd.read_sql_query(sql=text(query), con=conn)
    return df


engine = create_engine_pg()
t = text("UPDATE journals SET back_cal_time = now() WHERE uuid=:uuid")


@retry(stop=stop_after_attempt(30), wait=wait_fixed(1))
def innovation_assessment(id: str) -> dict:
    """Get results for a detaied innovation assessment from upload files"""

    id_list = [f"{id}_{i}" for i in range(151)]
    fetch_response = index.fetch(ids=id_list)

    docs = []
    for key, value in fetch_response["vectors"].items():
        docs.append(
            {
                "id": key,
                "vector": value["values"],
                "metadata": value["metadata"],
            }
        )

    with open(f"data/backdata/back_result_1.csv", mode="a+", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        for doc in docs:
            query_response = index.query(
                top_k=32,
                include_values=False,
                include_metadata=False,
                vector=doc["vector"],
                filter={
                    "$and": [
                        {"document_id": {"$ne": id}},
                        {"created_at": {"$gt": doc["metadata"]["created_at"]}},
                    ]
                },
            )
            result = [
                {
                    "id": match["id"],
                    "score": match["score"],
                }
                for match in query_response["matches"]
            ]
            scores_list = [doc["id"]] + [[item["id"], item["score"]] for item in result]
            writer.writerow(scores_list)

    with engine.connect() as connection:
        connection.execute(t, {"uuid": id})
        connection.commit()


query = "WITH ranked_articels AS (SELECT row_number() OVER (ORDER BY uuid) as row_num, uuid FROM journals WHERE (embedding_time IS NOT NULL AND authors IS NOT NULL)) SELECT row_num, uuid FROM ranked_articels WHERE row_num BETWEEN 100001 AND 200000"


df = select_from_db(engine, query)

for value in df["uuid"]:
    innovation_assessment(str(value))
