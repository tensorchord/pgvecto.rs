import pytest
from pgvector_rs.sqlalchemy import Vector
from sqlalchemy import create_engine, select, text, MetaData, Table, Column, Index, Integer
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import declarative_base, mapped_column, Session


@pytest.fixture(scope="module")
def engine():
    '''
    connect to the test db
    '''

    # TODO : create test table with script
    engine = create_engine(
        'postgresql+psycopg2://localhost/pgvector_test'
    )

    # ensure that we have installed pgvector.rs extension
    with engine.connect() as con:
        con.execute(text('CREATE EXTENSION IF NOT EXISTS vectors'))
        con.commit()
    return engine


@pytest.fixture(scope="module")
def metadata(engine):
    metadata = MetaData()
    metadata.drop_all(engine)
    metadata.bind = engine
    return metadata


@pytest.fixture(scope="module")
def test_table(metadata):
    return Table(
        'tb_test_item',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('embedding', Vector(3))
    )


@pytest.fixture(scope="module", autouse=True)
def create_test_table(test_table):
    '''
    create clean table for current db test before all tests
    '''
    test_table.create()
    try:
        yield
    finally:
        test_table.drop()
