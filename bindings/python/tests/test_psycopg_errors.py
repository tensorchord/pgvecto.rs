import numpy as np
import psycopg
import pytest
from psycopg import Connection

from pgvecto_rs.psycopg import register_vector
from pgvecto_rs.types import SparseVector
from tests import URL


@pytest.fixture()
def conn():
    with psycopg.connect(URL) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS vectors;")
        register_vector(conn)
        conn.execute("DROP TABLE IF EXISTS tb_test_svector;")
        conn.execute(
            "CREATE TABLE tb_test_svector (id bigserial PRIMARY KEY, embedding svector NOT NULL);"
        )
        conn.commit()
        try:
            yield conn
        finally:
            conn.execute("DROP TABLE IF EXISTS tb_test_svector;")
            conn.commit()


def test_copy_sparse_indices_fail(conn: Connection):
    with pytest.raises(ValueError, match="ndarray must be 1D for vector, got 2D"):
        conn.execute(
            "INSERT INTO tb_test_svector (embedding) VALUES (%b)",
            ([SparseVector(3, np.array([[0], [0]]), [1.0, 3.0, 4.0])]),
        )
    conn.rollback()


def test_copy_sparse_dims_fail(conn: Connection):
    with pytest.raises(
        ValueError, match="dims in SparseVector must be of type int, got float"
    ):
        conn.execute(
            "INSERT INTO tb_test_svector (embedding) VALUES (%b)",
            ([SparseVector(3.1, [0, 2], [1.0, 3.0, 4.0])]),
        )
    conn.rollback()


def test_copy_sparse_values_fail(conn: Connection):
    with pytest.raises(
        ValueError,
        match="values in SparseVector must be of type list or ndarray, got set",
    ):
        conn.execute(
            "INSERT INTO tb_test_svector (embedding) VALUES (%b)",
            ([SparseVector(3, [0, 2], set([4, 5, 6, 7]))]),
        )
    conn.rollback()


def test_copy_sparse_elements_fail(conn: Connection):
    with pytest.raises(
        ValueError,
        match="elements of indices in SparseVector must be of type int or integer, got float",
    ):
        conn.execute(
            "INSERT INTO tb_test_svector (embedding) VALUES (%b)",
            ([SparseVector(3, [0, 2.1], [1.0, 3.0, 4.0])]),
        )
    conn.rollback()


def test_copy_sparse_length_fail(conn: Connection):
    with pytest.raises(
        ValueError,
        match="sparse vector expected indices length 2 to match values length 3",
    ):
        conn.execute(
            "INSERT INTO tb_test_svector (embedding) VALUES (%b)",
            ([SparseVector(3, [0, 2], [1.0, 2.0, 3.0])]),
        )
    conn.rollback()
