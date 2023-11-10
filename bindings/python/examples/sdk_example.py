import os

from openai import OpenAI
from pgvecto_rs.sdk import PGVectoRs
from pgvecto_rs.sdk.embedder import OpenAIEmbedder

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

texts = [
    "Hello world!",
    "Hello PostgreSQL!",
    "Hello pgvecto.rs!",
]

client = PGVectoRs.from_texts(
    texts=texts,
    meta={"source": "sample.txt"},
    db_url=URL,
    table_name="sample_txt",
    dimension=embedder.get_dimension(),
    embedder=embedder,
)

try:
    # Query
    for record, dis in client.search(embedder.embed("hello pgvector")):
        print(f"DISTANCE SCORE: {dis}")
        print(record)
finally:
    # Clean up
    client.drop()
