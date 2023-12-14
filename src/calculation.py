import csv
import os

import pandas as pd
import pinecone
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed
from sqlalchemy import create_engine, text

load_dotenv()

pinecone.init(
    api_key=os.environ["PINECONE_API_KEY"],
    environment=os.environ["PINECONE_ENVIRONMENT"],
)
index = pinecone.Index(os.environ["PINECONE_INDEX"])

@retry(stop=stop_after_attempt(30), wait=wait_fixed(1))
def innovation_assessment(id: str) -> dict:
    """Get results for a detaied innovation assessment from upload files"""

    id_list = [f"{id}_{i}" for i in range(101)]
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

    with open("result.csv", mode="a+", newline="", encoding="utf-8") as file:
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
                        {"created_at": {"$lt": doc["metadata"]["created_at"]}},
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
            scores_list = [doc["id"]] + [item["score"] for item in result]
            writer.writerow(scores_list)


def create_engine_pg():
    engine = create_engine(
        os.environ["DATABASE_URL"],
    )
    return engine


def select_from_db(_engine, query):
    with _engine.begin() as conn:
        df = pd.read_sql_query(sql=text(query), con=conn)
    return df


query = "SELECT uuid FROM journals WHERE journal = 'RESOURCES, CONSERVATION AND RECYCLING' AND embedding_time IS NOT NULL AND authors IS NOT NULL"

engine = create_engine_pg()

df = select_from_db(engine, query)

for value in df["uuid"]:
    innovation_assessment(str(value))
