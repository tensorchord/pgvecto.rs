import os
import time

import numpy as np
import psycopg
import pyarrow as pa
from psycopg.adapt import Loader, Dumper
from psycopg.pq import Format
from psycopg.types import TypeInfo

from struct import pack, unpack

URL = "postgresql://{username}:{password}@{host}:{port}/{db_name}".format(
    port=os.getenv("DB_PORT", "5432"),
    host=os.getenv("DB_HOST", "localhost"),
    username=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASS", "mysecretpassword"),
    db_name=os.getenv("DB_NAME", "postgres"),
)


def to_db_binary(value: np.ndarray):
    if value is None:
        return value

    value = np.asarray(value, dtype='>f')

    if value.ndim != 1:
        raise ValueError("expected 1d array, not %d" % value.ndim)
    dims: bytes = pack('<H', value.shape[0], )
    return dims + value.tobytes()


def from_db_binary(value):
    if value is None:
        return value

    dim = unpack('<H', value[:2])[0]
    return np.frombuffer(value, dtype='>f', count=dim, offset=2).astype(np.float32)


class VectorDumper(Dumper):
    format = Format.BINARY

    def dump(self, obj):
        return to_db_binary(obj)


class VectorLoader(Loader):
    format = Format.BINARY

    def load(self, data):
        return from_db_binary(data)


def register_vector_info(context: psycopg.Connection, info: TypeInfo):
    if info is None:
        raise psycopg.ProgrammingError(
            info="vector type not found in the database")
    info.register(context)

    class VectorBinaryDumper(VectorDumper):
        oid = info.oid

    adapters = context.adapters
    adapters.register_dumper(np.ndarray, VectorBinaryDumper)
    adapters.register_loader(info.oid, VectorLoader)


def register_vector(context: psycopg.Connection):
    info = TypeInfo.fetch(context, "vector")
    register_vector_info(context, info)


def test_copy(conn: psycopg.Connection):
    # rows = 100000
    rows = 100
    # rows = 1
    dims = 1536
    conn.execute("DROP TABLE IF EXISTS testv;")
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS testv (id SERIAL PRIMARY KEY, embedding vector({dims}) NOT NULL);",
    )
    conn.commit()
    try:
        embedding = np.random.rand(rows, dims).astype(np.float32)
        # timer with milliseconds
        timer = time.time()
        # insert 10,000 rows into the table
        for i in range(rows):
            conn.execute(
                "INSERT INTO testv (embedding) VALUES (%b);",
                (embedding[i], ),
            )
        conn.commit()
        print(
            f"Insert {rows} rows in {(time.time() - timer)*1000:.3f} millseconds")

        # show the table rows
        cur = conn.execute("SELECT COUNT(*) FROM testv;")
        print(cur.fetchone()[0])

        timer = time.time()
        # insert 10,000 rows using copy by row
        cursor = conn.cursor()
        with cursor.copy("COPY testv (embedding) FROM STDIN WITH BINARY") as copy:
            copy.set_types(['vector'])
            for i in range(rows):
                copy.write_row([embedding[i]])
        print(
            f"Copy {rows} rows by row in {(time.time() - timer)*1000:.3f} millseconds")

        # show the table size
        cur = conn.execute("SELECT COUNT(*) FROM testv;")
        print(cur.fetchone()[0])

        # # insert 10,000 rows using copy by block
        # bytes = b''
        # for i in range(rows):
        #     bytes += to_db_binary(embedding[i])
        # timer = time.time()
        # # insert 10,000 rows using copy by row
        # cursor = conn.cursor()
        # with cursor.copy("COPY testv (embedding) FROM STDIN WITH BINARY") as copy:
        #     copy.write(bytes)
        # print(
        #     f"Copy by block {rows} rows by block in {(time.time() - timer)*1000:.3f} millseconds")

    finally:
        # Drop the table
        conn.execute("DROP TABLE IF EXISTS testv;")
        conn.commit()


# Connect to the DB and init things
with psycopg.connect(URL) as conn:
    conn.execute("CREATE EXTENSION IF NOT EXISTS vectors;")
    register_vector(conn)
    conn.execute("DROP TABLE IF EXISTS documents;")
    conn.execute(
        "CREATE TABLE documents (id SERIAL PRIMARY KEY, embedding vector(3) NOT NULL);",
    )
    conn.commit()
    try:
        # Insert 3 rows into the table
        conn.execute(
            "INSERT INTO documents (embedding) VALUES (%b);",
            (np.array([1, 2, 3]), ),
        )
        conn.execute(
            "INSERT INTO documents (embedding) VALUES (%b);",
            (np.array([1.0, 2.0, 4.0]), ),
        )
        conn.execute(
            "INSERT INTO documents (embedding) VALUES (%b);",
            (np.array([1, 3, 4]), ),
        )
        conn.commit()

        # Select the row "hello pgvecto.rs"
        cur = conn.execute(
            "SELECT * FROM documents;", binary=True,
        )
        for row in cur.fetchall():
            print(row[1])
    finally:
        # Drop the table
        conn.execute("DROP TABLE IF EXISTS documents;")
        conn.commit()

    test_copy(conn)
