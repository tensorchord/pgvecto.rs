import os

import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from pgvecto_rs.psycopg import register_vector

URL = "postgresql://{username}:{password}@{host}:{port}/{db_name}".format(
    port=os.getenv("DB_PORT", "5432"),
    host=os.getenv("DB_HOST", "localhost"),
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASS", "mysecretpassword"),
    db_name=os.getenv("DB_NAME", "postgres"),
)


def copy_by_row(conn: psycopg.Connection):
    conn.execute("DROP TABLE IF EXISTS documents;")
    conn.execute(
        "CREATE TABLE documents (id SERIAL PRIMARY KEY, embedding vector(3) NOT NULL);",
    )
    conn.commit()
    try:
        # create a parquet file , and write row into it.
        table = pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "embedding": [
                        np.array([1, 2, 3]),
                        np.array([1.0, 2.0, 4.0]),
                        np.array([1, 3, 4]),
                    ]
                }
            )
        )
        pq.write_table(table, "test.parquet")

        # load vectors from parquet file
        table = pq.read_table("test.parquet")
        # TODO: Is there a better way to convert pyarrow table to numpy array to reduce copy overhead?
        embeddings = table.column("embedding").to_numpy()

        with conn.cursor() as cursor, cursor.copy(
            "COPY documents (embedding) FROM STDIN (FORMAT BINARY)"
        ) as copy:
            # write row by row
            for e in embeddings:
                copy.write_row([e])
            copy.write_row([np.array([1, 3, 5])])
        conn.commit()

        # Select the row "hello pgvecto.rs" using binary format
        cur = conn.execute(
            "SELECT * FROM documents;",
            binary=True,
        )
        for row in cur.fetchall():
            print(row[0], ": ", row[1])

        # output will be:
        # 1 :  [1.0, 2.0, 3.0]
        # 2 :  [1.0, 2.0, 4.0]
        # 3 :  [1.0, 3.0, 4.0]
        # 4 :  [1.0, 3.0, 5.0]
    finally:
        # Drop the table
        # conn.execute("DROP TABLE IF EXISTS documents;")
        conn.commit()


# Connect to the DB and init things
with psycopg.connect(URL) as conn:
    conn.execute("CREATE EXTENSION IF NOT EXISTS vectors;")
    register_vector(conn)

    copy_by_row(conn)
