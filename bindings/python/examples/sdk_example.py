import os

from openai import OpenAI
from pgvecto_rs.sdk import PGVectoRs, Record, filters

URL = "postgresql+psycopg://{username}:{password}@{host}:{port}/{db_name}".format(
    port=os.getenv("DB_PORT", 5432),
    host=os.getenv("DB_HOST", "localhost"),
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASS", "mysecretpassword"),
    db_name=os.getenv("DB_NAME", "postgres"),
)
embedding = OpenAI().embeddings


def embed(text: str):
    return (
        embedding.create(input=text, model="text-embedding-ada-002").data[0].embedding
    )


texts = [
    "Hello world",
    "Hello PostgreSQL",
    "Hello pgvecto.rs!",
]
records1 = [Record.from_text(text, {"src": "one"}, embed(text)) for text in texts]
records2 = [Record.from_text(text, {"src": "two"}, embed(text)) for text in texts]
target = embed("Hello vector database!")

# Create an empty client
client = PGVectoRs(
    db_url=URL,
    table_name="example",
    dimension=1536,
)
try:
    # Add some records
    client.add_records(records1)
    client.add_records(records2)

    # Query (With a filter from the filters module)
    print("#################### First Query ####################")
    for record, dis in client.search(
        target, filter=filters.meta_contains({"src": "one"})
    ):
        print(f"DISTANCE SCORE: {dis}")
        print(record)

    # Another Query (Equivalent to the first one, but with a lambda filter written by hand)
    print("#################### Second Query ####################")
    for record, dis in client.search(
        target, filter=lambda r: r.meta.contains({"src": "one"})
    ):
        print(f"DISTANCE SCORE: {dis}")
        print(record)

    # Yet Another Query (With a more complex filter)
    print("#################### Third Query ####################")

    def complex_filter(r: filters.FilterInput) -> filters.FilterOutput:
        t1 = r.text.endswith("!") == False  # noqa: E712
        t2 = r.meta.contains({"src": "two"})
        t = t1 & t2
        return t

    for record, dis in client.search(target, filter=complex_filter):
        print(f"DISTANCE SCORE: {dis}")
        print(record)

finally:
    # Clean up
    client.drop()
