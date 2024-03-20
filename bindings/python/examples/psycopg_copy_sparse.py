import os

import numpy as np
import psycopg

from pgvecto_rs.psycopg import register_vector
from pgvecto_rs.types import SparseVector

URL = "postgresql://{username}:{password}@{host}:{port}/{db_name}".format(
    port=os.getenv("DB_PORT", "5432"),
    host=os.getenv("DB_HOST", "localhost"),
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASS", "mysecretpassword"),
    db_name=os.getenv("DB_NAME", "postgres"),
)


# Connect to the DB and init things
with psycopg.connect(URL) as conn:
    conn.execute("CREATE EXTENSION IF NOT EXISTS vectors;")
    register_vector(conn)
    conn.execute("DROP TABLE IF EXISTS documents;")
    conn.execute(
        "CREATE TABLE documents (id SERIAL PRIMARY KEY, embedding svector NOT NULL);",
    )
    conn.commit()
    try:
        with conn.cursor() as cursor, cursor.copy(
            "COPY documents (embedding) FROM STDIN (FORMAT BINARY)"
        ) as copy:
            copy.write_row([SparseVector(3, [0, 2], [1.0, 3.0])])
            copy.write_row([SparseVector(3, np.array([0, 1, 2]), [1.0, 2.0, 3.0])])
            copy.write_row([SparseVector(3, np.array([1, 2]), np.array([2.0, 3.0]))])
        conn.pgconn.flush()
        conn.commit()

        # Select the rows using binary format
        cur = conn.execute(
            "SELECT * FROM documents;",
            binary=True,
        )
        for row in cur.fetchall():
            print(row[0], ": ", row[1])

        # output will be:
        # 1 :  [1.0, 0.0, 3.0]
        # 2 :  [1.0, 2.0, 3.0]
        # 3 :  [0.0, 2.0, 3.0]
    finally:
        # Drop the table
        conn.execute("DROP TABLE IF EXISTS documents;")
        conn.commit()
