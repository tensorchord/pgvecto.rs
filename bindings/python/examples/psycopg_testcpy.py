import os
import time

import numpy as np
import psycopg
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq
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


def test_insert(conn: psycopg.Connection, embedding: np.ndarray, rows: int, dims: int):
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
    print(f"insert {cur.fetchone()[0]} rows")


def test_copy_by_block(conn: psycopg.Connection, embedding: np.ndarray, rows: int, dims: int):
    # insert 10,000 rows using copy by block
    bytes = b''
    with conn.cursor() as cursor:
        with cursor.copy("COPY testv (embedding) TO STDOUT WITH BINARY") as copy:
            for data in copy:
                bytes += data
    print(f"Bytes size: {len(bytes)}")
    # clear the table
    conn.execute("TRUNCATE TABLE testv;")
    conn.commit()
    timer = time.time()
    with conn.cursor() as cursor:
        cursor = conn.cursor()
        with cursor.copy("COPY testv (embedding) FROM STDIN WITH BINARY") as copy:
            copy.write(bytes)
    print(
        f"Copy {rows} rows by block in {(time.time() - timer)*1000:.3f} millseconds")
    # show the table size
    cur = conn.execute("SELECT COUNT(*) FROM testv;")
    print(f"insert {cur.fetchone()[0]} rows")


def test_copy_by_row(conn: psycopg.Connection, embedding: np.ndarray, rows: int, dims: int):
    # clear the table
    conn.execute("TRUNCATE TABLE testv;")
    conn.commit()
    timer = time.time()
    # insert 10,000 rows using copy by row
    with conn.cursor() as cursor:
        with cursor.copy("COPY testv (embedding) FROM STDIN WITH BINARY") as copy:
            copy.set_types(['vector'])
            for i in range(rows):
                copy.write_row([embedding[i]])
    print(
        f"Copy {rows} rows by row in {(time.time() - timer)*1000:.3f} millseconds")

    # show the table size
    cur = conn.execute("SELECT COUNT(*) FROM testv;")
    print(f"insert {cur.fetchone()[0]} rows")


def test_insert_parquet(embedding: np.ndarray, rows: int, dims: int):
    # write array to a local parquet file, vector as a single column
    table = pa.Table.from_pandas(
        pd.DataFrame({'embedding': embedding.tolist()}))
    timer = time.time()
    pq.write_table(table, 'testv.parquet')
    print(
        f"Write {rows} rows to parquet in {(time.time() - timer)*1000:.3f} millseconds")


def benchs(conn: psycopg.Connection):
    rows = 400000
    # rows = 1000
    # rows = 1
    dims = 1536
    conn.execute("DROP TABLE IF EXISTS testv;")
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS testv (id SERIAL PRIMARY KEY, embedding vector({dims}) NOT NULL);",
    )
    conn.execute("alter table testv alter embedding set storage external;")
    conn.commit()
    try:
        embedding = np.random.rand(rows, dims).astype(np.float32)
        # test_copy_by_row(conn, embedding, rows, dims)
        test_insert(conn, embedding, rows, dims)

        # example result:
        # Insert 1000 rows in 154.197 millseconds
        # 1000
        # Bytes size: 6152021 > (dims * 4 + 2) * rows
        # Copy 1000 rows by block in 26.903 millseconds
        # 1000
        # Copy 1000 rows by row in 26.125 millseconds
        # 1000
        # Write 1000 rows to parquet in 55.226 millseconds

    finally:
        # Drop the table
        conn.execute("DROP TABLE IF EXISTS testv;")
        conn.commit()


def test_basic(conn: psycopg.Connection):
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


# Connect to the DB and init things
with psycopg.connect(URL) as conn:
    conn.execute("CREATE EXTENSION IF NOT EXISTS vectors;")
    register_vector(conn)
    conn.execute("DROP TABLE IF EXISTS documents;")

    benchs(conn)
