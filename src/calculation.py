import os

import pinecone
from dotenv import load_dotenv

load_dotenv()

pinecone.init(
    api_key=os.environ["PINECONE_API_KEY"],
    environment=os.environ["PINECONE_ENVIRONMENT"],
)
index = pinecone.Index(os.environ["PINECONE_INDEX"])


def innovation_assessment(id: str) -> dict:
    """Get results for a detaied innovation assessment from upload files"""

    id_list = [f"{id}_{i}" for i in range(101)]
    fetch_response = index.fetch(ids=id_list)

    docs = []
    for key, value in fetch_response['vectors'].items():
        docs.append(
            {
                "id": key,
                "vector": value["values"],
                # "metadata": value["metadata"],
            }
        )


    for doc in docs:
        query_response = index.query(
            top_k=32,
            include_values=False,
            include_metadata=False,
            vector=doc["vector"],
        )
        print(query_response)
        result = [
            {
                "id": match["id"],
                "metadata": match["metadata"],
            }
            for match in query_response["matches"]
        ]



    docs_list = []
    chunk_scores = []
    for chunk in st.session_state["doc_chucks"]:
        docs = vectorstore.similarity_search_with_score(chunk.page_content, k=5)
        score = 0
        for doc in docs:
            date = datetime.datetime.fromtimestamp(doc[0].metadata["created_at"])
            formatted_date = date.strftime("%Y-%m")  # Format date as 'YYYY-MM'
            source_entry = "[{}. {}.]({})".format(
                doc[0].metadata["source_id"],
                formatted_date,
                doc[0].metadata["url"],
            )
            docs_list.append(
                {
                    "authors": doc[0].metadata["author"],
                    "source": source_entry,
                    "score": doc[1],
                }
            )
            score += doc[1]
        # Store the score and the chunk together
        chunk_scores.append({"score": score, "chunk": chunk.page_content})
    # Sort the list based on score in ascending order (lowest first)
    sorted_chunk_scores = sorted(chunk_scores, key=lambda x: x["score"])
    # Get the 16 chunks with the lowest scores along with their scores
    lowest_score_entries = sorted_chunk_scores[:16]
    lowest_score_chunks = [entry["chunk"] for entry in lowest_score_entries]
    # Sum the scores for the 16 chunks with the lowest scores
    # 去掉最高分（不要文本过短可能带来的极值）
    sum_of_lowest_scores = sum([entry["score"] for entry in lowest_score_entries])
    finally_score = round((100 * (90 - sum_of_lowest_scores) / 90), 2)

    # 使用一个字典来聚合每个 `source` 的总分和作者信息。
    source_scores = {}
    for doc in docs_list:
        source = doc["source"]
        if source not in source_scores:
            source_scores[source] = {"score": 0, "authors": doc["authors"]}
        else:
            source_scores[source]["score"] += doc["score"]

    # 将字典转换为一个列表。
    source_score_list = [
        {"source": key, "total_score": value["score"], "authors": value["authors"]}
        for key, value in source_scores.items()
    ]

    # 根据总分对列表进行排序。
    sorted_source_score_list = sorted(
        source_score_list, key=lambda x: x["total_score"], reverse=True
    )

    # 提取分数最高的10个 `source`，并带上authors的信息。
    top_10_sources_by_score = [
        {"authors": entry["authors"], "source": entry["source"]}
        for entry in sorted_source_score_list[:10]
    ]

    llm_chat = ChatOpenAI(
        model=llm_model,
        temperature=0,
        streaming=False,
        verbose=langchain_verbose,
    )
    prompt_potential_innovation_msgs = [
        SystemMessage(
            content="""Summarize potential innovations from text snipets. Use bullet points if a better expression effect can be achieved."""
        ),
        HumanMessage(content="The text snipets:"),
        HumanMessagePromptTemplate.from_template("{input}"),
    ]

    prompt_potential_innovation = ChatPromptTemplate(
        messages=prompt_potential_innovation_msgs
    )
    chain = LLMChain(llm=llm_chat, prompt=prompt_potential_innovation)
    potential_innovations = chain.run(lowest_score_chunks)

    result = {
        "potential_innovations": potential_innovations,
        "innovation_score": finally_score,
        "recommended_reviewers_and_their_relevant_published_articles": top_10_sources_by_score,
    }

    return result


innovation_assessment("1f11f4d2-ce79-47e5-b034-382b1d28e88a")
