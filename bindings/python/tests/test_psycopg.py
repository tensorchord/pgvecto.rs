import numpy as np
import psycopg
import pytest
from psycopg import Connection, sql

from pgvecto_rs.psycopg import register_vector
from pgvecto_rs.types import SparseVector
from tests import (
    EXPECTED_NEG_COS_DIS,
    EXPECTED_NEG_DOT_PROD_DIS,
    EXPECTED_SQRT_EUCLID_DIS,
    LEN_AFT_DEL,
    OP_NEG_COS_DIS,
    OP_NEG_DOT_PROD_DIS,
    OP_SQRT_EUCLID_DIS,
    TOML_SETTINGS,
    URL,
    VECTORS,
)


@pytest.fixture(scope="module")
def conn():
    with psycopg.connect(URL) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS vectors;")
        register_vector(conn)
        conn.execute("DROP TABLE IF EXISTS tb_test_item;")
        conn.execute(
            "CREATE TABLE tb_test_item (id bigserial PRIMARY KEY, embedding vector(3) NOT NULL);",
        )
        conn.commit()
        try:
            yield conn
        finally:
            conn.execute("DROP TABLE IF EXISTS tb_test_item;")
            conn.commit()


@pytest.mark.parametrize(("index_name", "index_setting"), TOML_SETTINGS.items())
def test_create_index(conn: Connection, index_name: str, index_setting: str):
    stat = sql.SQL(
        "CREATE INDEX {} ON tb_test_item USING vectors (embedding vector_l2_ops) WITH (options={});",
    ).format(sql.Identifier(index_name), index_setting)

    conn.execute(stat)
    conn.commit()


# The server cannot handle invalid vectors currently, see https://github.com/tensorchord/pgvecto.rs/issues/96
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


def test_copy(conn: Connection):
    with conn.cursor() as cursor, cursor.copy(
        "COPY tb_test_item (embedding) FROM STDIN (FORMAT BINARY)"
    ) as copy:
        for e in VECTORS:
            copy.write_row([e])

    conn.commit()
    cur = conn.execute("SELECT * FROM tb_test_item;", binary=True)
    rows = cur.fetchall()
    assert len(rows) == len(VECTORS)
    for i, e in enumerate(rows):
        assert np.allclose(e[1], VECTORS[i], atol=1e-10)
    conn.execute("Delete FROM tb_test_item;")
    conn.commit()


def test_copy_sparse(conn: Connection):
    conn.execute("DROP TABLE IF EXISTS tb_test_svector;")
    conn.execute(
        "CREATE TABLE tb_test_svector (id bigserial PRIMARY KEY, embedding svector NOT NULL);"
    )
    conn.commit()
    try:
        rows_number = 0
        with conn.cursor() as cursor, cursor.copy(
            "COPY tb_test_svector (embedding) FROM STDIN (FORMAT BINARY)"
        ) as copy:
            copy.write_row([SparseVector(3, [0, 2], [1.0, 3.0])])
            copy.write_row([SparseVector(3, np.array([0, 1, 2]), [1.0, 2.0, 3.0])])
            copy.write_row([SparseVector(3, np.array([1, 2]), np.array([2.0, 3.0]))])
        conn.commit()
        rows_number = 3
        cur = conn.execute("SELECT * FROM tb_test_svector;", binary=True)
        rows = cur.fetchall()
        assert len(rows) == rows_number
        assert str(rows[0][1]) == "[1.0, 0.0, 3.0]"
        assert str(rows[1][1]) == "[1.0, 2.0, 3.0]"
        assert str(rows[2][1]) == "[0.0, 2.0, 3.0]"
        conn.commit()

    finally:
        conn.execute("DROP TABLE IF EXISTS tb_test_svector;")
        conn.commit()


def test_insert(conn: Connection):
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO tb_test_item (embedding) VALUES (%s);",
            [(e,) for e in VECTORS],
        )
        cur.execute("SELECT * FROM tb_test_item;")
        conn.commit()
        rows = cur.fetchall()
        assert len(rows) == len(VECTORS)
        for i, e in enumerate(rows):
            assert np.allclose(e[1], VECTORS[i], atol=1e-10)


def test_squared_euclidean_distance(conn: Connection):
    cur = conn.execute(
        "SELECT embedding <-> %s FROM tb_test_item;",
        (OP_SQRT_EUCLID_DIS,),
    )
    for i, row in enumerate(cur.fetchall()):
        assert np.allclose(EXPECTED_SQRT_EUCLID_DIS[i], row[0], atol=1e-10)


def test_negative_dot_product_distance(conn: Connection):
    cur = conn.execute(
        "SELECT embedding <#> %s FROM tb_test_item;",
        (OP_NEG_DOT_PROD_DIS,),
    )
    for i, row in enumerate(cur.fetchall()):
        assert np.allclose(EXPECTED_NEG_DOT_PROD_DIS[i], row[0], atol=1e-10)


def test_negative_cosine_distance(conn: Connection):
    cur = conn.execute("SELECT embedding <=> %s FROM tb_test_item;", (OP_NEG_COS_DIS,))
    for i, row in enumerate(cur.fetchall()):
        assert np.allclose(EXPECTED_NEG_COS_DIS[i], row[0], atol=1e-10)


# # =================================
# # Suffix functional tests
# # =================================


def test_delete(conn: Connection):
    conn.execute("DELETE FROM tb_test_item WHERE embedding = %s;", (VECTORS[0],))
    conn.commit()
    cur = conn.execute("SELECT * FROM tb_test_item;")
    assert len(cur.fetchall()) == LEN_AFT_DEL
