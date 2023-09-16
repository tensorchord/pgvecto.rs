import pytest
import numpy as np
from tests import URL
from pgvector_rs.sqlalchemy import Vector
from sqlalchemy import create_engine, select, text, MetaData, Table, Column, Index, Integer
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import declarative_base, mapped_column, Session


@pytest.fixture(scope='module')
def engine():
    '''
    Connect to the test db pointed by the URL. Can check more details
    in `tests/__init__.py`
    '''

    engine = create_engine(URL)

    # ensure that we have installed pgvector.rs extension
    with engine.connect() as con:
        con.execute(text('CREATE EXTENSION IF NOT EXISTS vectors'))
        con.commit()
    return engine


@pytest.fixture(scope='module')
def metadata(engine):
    metadata = MetaData()
    metadata.drop_all(engine)
    metadata.bind = engine
    return metadata


@pytest.fixture(scope='module')
def test_table(metadata):
    return Table(
        'tb_test_item',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('embedding', Vector(3))
    )


@pytest.fixture(scope='module', autouse=True)
def create_test_table(test_table, engine):
    '''
    Create clean table for current db test before all tests.
    Note: this table will be dropped after finishing all tests.
    '''
    test_table.create(engine)
    try:
        yield
    finally:
        test_table.drop(engine)

def test_create_index(test_table, engine):
    toml_settings = {
        'hnsw': """$$
capacity = 2097152
[vectors]
memmap = "ram"
[algorithm.hnsw]
memmap = "ram"
$$
""",
        'ivf':  """$$
capacity = 2097152
[vectors]
memmap = "ram"
[algorithm.ivf]
memmap = "ram"
nlist = 1000
nprobe = 10
$$
"""
    }
    index = Index(
        'test_vector_index',
        test_table.c.embedding,
        postgresql_using='vectors',
        postgresql_with={'options': toml_settings['hnsw']},
        postgresql_ops={'embedding': 'l2_ops'}
    )
    index.create(engine)


def test_invalid_insert(test_table, engine):
    _invalid_vectors = [
        [1, 2, 3, 4],
        [1,],
        ['123.', '123', 'a'],
        np.array([1, 2, 3, 4]),
        np.array([1, '3', 3]),
        np.zeros(shape=(1, 2)),
    ]
    with engine.connect() as con:
        for i, e in enumerate(_invalid_vectors):
            try:
                con.execute(
                    test_table.insert().values(
                        {'id': i, 'embedding': e}
                    )
                )
            except:
                continue
            assert(
                False,
                'failed to raise invalid value error for {}th vector {}'
                .format(i, e),
            )


# =================================
# Tests share the same vectors
# =================================
_vectors = [
    [1, 2, 3],
    [0., -45, 2.34],
    np.ones(shape=(3)),
]


def test_insert(test_table, engine):
    with engine.connect() as con:
        con.execute(
            test_table.insert().values(
                [{'id': i, 'embedding': e} for i, e in enumerate(_vectors)]
            )
        )
        for row in con.execute(test_table.select()):
            assert(
                np.allclose(row.embedding, _vectors[row.id], atol=1e-10)
            )
        con.commit()


def test_squared_euclidean_distance(test_table, engine):
    # value excuted from psql
    _expected = [14.0, 2030.4756, 3.0]
    with engine.connect() as con:
        for row in enumerate(con.execute(
                select(test_table.c.id, test_table.c.embedding.squared_euclidean_distance([0, 0, 0])))):
            (i, res) = row
            assert(np.allclose(_expected[i], res, atol=1e-10),
                    "incorrect calculation result for {}th vector {}".format(i, _vectors[i]))
        con.commit()


def test_negative_dot_product_distance(test_table, engine):
    # value excuted from psql
    _expected = [-17.0, 80.64, -7.0]
    with engine.connect() as con:
        for row in enumerate(con.execute(
            select(test_table.c.embedding.negative_dot_product_distance([1, 2, 4])))
        ):
            (i, res)=row
            assert(np.allclose(_expected[i], res, atol=1e-10),
                    "incorrect calculation result for {}th vector {}".format(i, _vectors[i]))
        con.commit()


def test_negative_cosine_distance(test_table, engine):
    # value excuted from psql
    _expected = [-10.0, 87.66, -6.0]
    with engine.connect() as con:
        for row in enumerate(con.execute(
            select(test_table.c.embedding.negative_cosine_distance([3, 2, 1])))
        ):
            (i, res)=row
            assert(np.allclose(_expected[i], res, atol=1e-10),
                    "incorrect calculation result for {}th vector {}".format(i, _vectors[i]))
        con.commit()

def test_delete(test_table, engine):
    with engine.connect() as con:
        con.execute(test_table.delete().where(test_table.c.embedding.__eq__([1, 2, 3])))
        result = con.execute(test_table.select())
        assert(len(list(result))==2)
        con.commit()
