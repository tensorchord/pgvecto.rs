import os
from pgvecto_rs.highapi import Client, Record, RecordORMType
from pgvecto_rs.highapi.embedder import OpenAIEmbedder
from openai import OpenAI

URL = "postgresql+psycopg://{username}:{password}@{host}:{port}/{db_name}".format(
    port=os.getenv("DB_PORT", 5432),
    host=os.getenv("DB_HOST", "localhost"),
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASS", "mysecretpassword"),
    db_name=os.getenv("DB_NAME", "postgres"),
)


embedder = OpenAIEmbedder(
    OpenAI(),
    "text-embedding-ada-002",
)
with open("./examples/data/sample.txt", "r") as f:
    texts = f.readlines()

client = Client.from_texts(
    texts=texts,
    meta={"source": "sample.txt"},
    db_url=URL,
    table_name="sample_txt",
    dimension=embedder.get_dimension(),
    embedder=embedder,
)

# Query
for record, dis in client.search(embedder.embed("hello pgvector")):
    print(f"DISTANCE SCORE: {dis}")
    print(record)

# Clean up
client.drop()
