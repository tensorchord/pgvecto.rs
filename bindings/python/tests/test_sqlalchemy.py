import pytest
from pgvector_rs.sqlalchemy import Vector
from sqlalchemy import create_engine, select, text, MetaData, Table, Column, Index, Integer
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import declarative_base, mapped_column, Session

# TODO : create test table with script
engine = create_engine(
    'postgresql+psycopg2://localhost/pgvector_test')
with engine.connect() as con:
    con.execute(text('CREATE EXTENSION IF NOT EXISTS vectors'))
    con.commit()


def test_create_db():
    metadata = MetaData()

    item_table = Table(
        'core_item',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('embedding', Vector(3))
    )

    metadata.drop_all(engine)
    metadata.create_all(engine)
