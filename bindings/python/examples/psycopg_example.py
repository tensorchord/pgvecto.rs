import os

import numpy as np
import psycopg

from pgvecto_rs.psycopg import register_vector

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
    conn.execute(
        "CREATE TABLE documents (id SERIAL PRIMARY KEY, text TEXT NOT NULL, embedding vector(3) NOT NULL);",
    )
    conn.commit()
    try:
        # Insert 3 rows into the table
        conn.execute(
            "INSERT INTO documents (text, embedding) VALUES (%s, %s);",
            ("hello world", [1, 2, 3]),
        )
        conn.execute(
            "INSERT INTO documents (text, embedding) VALUES (%s, %s);",
            ("hello postgres", [1.0, 2.0, 4.0]),
        )
        conn.execute(
            "INSERT INTO documents (text, embedding) VALUES (%s, %s);",
            ("hello pgvecto.rs", np.array([1, 3, 4])),
        )
        conn.commit()

        # Select the row "hello pgvecto.rs"
        cur = conn.execute(
            "SELECT * FROM documents WHERE text = %s;",
            ("hello pgvecto.rs",),
        )
        target = cur.fetchone()[2]

        # Select all the rows and sort them
        # by the squared_euclidean_distance to "hello pgvecto.rs"
        cur = conn.execute(
            "SELECT text, embedding <-> %s AS distance FROM documents ORDER BY distance;",
            (target,),
        )
        for row in cur.fetchall():
            print(row)
        # The output will be:
        # ```
        # ('hello pgvecto.rs', 0.0)
        # ('hello postgres', 1.0)
        # ('hello world', 2.0)
        # ```
    finally:
        # Drop the table
        conn.execute("DROP TABLE IF EXISTS documents;")
        conn.commit()
