import pytest
import psycopg
import numpy as np
from psycopg import Connection
from pgvecto_rs.psycopg import register_vector
from tests import (
    URL,
    VECTORS,
)


@pytest.fixture(scope="module")
def conn():
    with psycopg.connect(URL) as conn:
        register_vector(conn)
        conn.execute("CREATE EXTENSION IF NOT EXISTS vectors;")
        conn.execute("DROP TABLE IF EXISTS tb_test_item")
        conn.execute(
            "CREATE TABLE tb_test_item (id bigserial PRIMARY KEY, embedding vector(3) NOT NULL);"
        )
        conn.commit()
        try:
            yield conn
        finally:
            conn.execute("DROP TABLE IF EXISTS tb_test_item")
            conn.commit()


# The server cannot handle invalid vectors curently, see https://github.com/tensorchord/pgvecto.rs/issues/96
# def test_invalid_insert(conn: Connection):
#     for i, e in enumerate(INVALID_VECTORS):
#         try:
#             conn.execute("INSERT INTO tb_test_item (embedding) VALUES (%s);", (e, ) )
#             pass
#         except:
#             conn.rollback()
#         else:
#             conn.rollback()
#             raise AssertionError(
#                 'failed to raise invalid value error for {}th vector {}'
#                 .format(i, e),
#             )

# =================================
# Semetic search tests
# =================================


def test_insert(conn: Connection):
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO tb_test_item (embedding) VALUES (%s);", [(e,) for e in VECTORS]
        )
        cur.execute("SELECT * FROM tb_test_item;")
        conn.commit()
        rows = cur.fetchall()
        assert len(rows) == len(VECTORS)
        for i, e in enumerate(rows):
            assert np.allclose(e[1], VECTORS[i], atol=1e-10)
