import pytest
import numpy as np
from pgvector_rs.sqlalchemy import Vector
from sqlalchemy import create_engine, select, text, MetaData, Table, Column, Index, Integer
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import declarative_base, mapped_column, Session


@pytest.fixture(scope='module')
def engine():
    '''
    connect to the test db
    '''

    # TODO : create test table with script. This below is just the placeholder
    engine = create_engine(
        'postgresql+psycopg2://localhost/pgvector_test'
    )

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
    create clean table for current db test before all tests
    '''
    test_table.create(engine)
    try:
        yield
    finally:
        test_table.drop(engine)


def test_insert(test_table, engine):
    _vectors = [
        [1, 2, 3],
        [0., -45, 2.34],
        np.ones(shape=(3)),
    ]

    with engine.connect() as con:
        con.execute(
            test_table.insert().values(
                [{'id': i, 'embedding': e} for i, e in enumerate(_vectors)]
            )
        )
        for row in con.execute(test_table.select()):
            assert (
                np.allclose(row.embedding, _vectors[row.id], atol=1e-10)
            )
        con.commit()


def test_invalid_insert(test_table, engine):
    _vectors = [
        [1, 2, 3, 4],
        [1,],
        ['123.', '123', 'a'],
        np.array([1, 2, 3, 4]),
        np.array([1, '3', 3]),
        np.zeros(shape=(1, 2)),
    ]
    with engine.connect() as con:
        for i, e in enumerate(_vectors):
            try:
                con.execute(
                    test_table.insert().values(
                        {'id': i, 'embedding': e}
                    )
                )
            except:
                continue
            assert (
                False,
                'failed to raise invalid value error for {}th vector {}'
                .format(i, e),
            )
