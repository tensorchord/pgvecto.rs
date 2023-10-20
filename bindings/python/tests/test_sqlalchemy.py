import pytest
import numpy as np
from tests import *
from sqlalchemy import create_engine, select, text, insert, delete
from sqlalchemy import Integer, Index
from pgvector_rs.sqlalchemy import Vector
from sqlalchemy.orm import Session, DeclarativeBase, mapped_column, Mapped


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "tb_test_item"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    embedding: Mapped[np.ndarray] = mapped_column(Vector(3))

    def __repr__(self) -> str:
        return f"{self.embedding}"

@pytest.fixture(scope='module')
def session():
    '''
    Connect to the test db pointed by the URL. Can check more details
    in `tests/__init__.py`
    '''

    engine = create_engine(URL)

    # ensure that we have installed pgvector.rs extension
    with engine.connect() as conn:
        conn.execute(text('CREATE EXTENSION IF NOT EXISTS vectors'))
        conn.execute(text('DROP TABLE IF EXISTS tb_test_item'))
        conn.commit()

    with Session(engine) as session:
        Document.metadata.create_all(engine)
        try:
            yield session
        finally:
            session.rollback()
            Document.metadata.drop_all(engine)


# =================================
# Prefix functional tests
# =================================

def test_create_index(session: Session):
    index = Index(
        'test_vector_index',
        Document.embedding,
        postgresql_using='vectors',
        postgresql_with={'options': TOML_SETTINGS['hnsw']},
        postgresql_ops={'embedding': 'l2_ops'}
    )
    index.create(session.bind, checkfirst=True)
    session.commit()


def test_invalid_insert(session: Session):
    for i, e in enumerate(INVALID_VECTORS):
        try:
            session.execute(insert(Document).values(id = i, embedding=e))
        except:
            session.rollback()
            continue
        session.rollback()
        raise AssertionError(
            'failed to raise invalid value error for {}th vector {}'
            .format(i, e),
        )

# =================================
# Semetic search tests
# =================================

def test_insert(session: Session):
    for stat in [insert(Document).values(id = i, embedding=e) for i, e in enumerate(VECTORS)]:
        session.execute(stat)
    session.commit()
    for row in session.scalars(select(Document)):
        assert(
            np.allclose(row.embedding, VECTORS[row.id], atol=1e-10)
        )

def test_squared_euclidean_distance(session: Session):
    for row in session.execute(
        select(Document.id, Document.embedding.squared_euclidean_distance([0, 0, 0]))
        ):
        (i, res) = row
        assert(np.allclose(EXPECTED_SQRT_EUCLID_DIS[i], res, atol=1e-10),
                "incorrect calculation result for {}th vector {}".format(i, VECTORS[i]))


def test_negative_dot_product_distance(session: Session):
    for row in session.execute(
        select(Document.id, Document.embedding.negative_dot_product_distance([0, 0, 0]))
        ):
        (i, res) = row
        assert(np.allclose(EXPECTED_NEG_DOT_PROD_DIS[i], res, atol=1e-10),
                "incorrect calculation result for {}th vector {}".format(i, VECTORS[i]))

def test_negative_cosine_distance(session: Session):
    for row in session.execute(
        select(Document.id, Document.embedding.negative_cosine_distance([0, 0, 0]))
        ):
        (i, res) = row
        assert(np.allclose(EXPECTED_NEG_COS_DIS[i], res, atol=1e-10),
                "incorrect calculation result for {}th vector {}".format(i, VECTORS[i]))

# # =================================
# # Suffix functional tests
# # =================================

def test_delete(session: Session):
    session.execute(delete(Document).where(Document.embedding == [1, 2, 3]))
    session.commit()
    res = session.execute(select(Document))
    assert(len(list(res))==LEN_AFT_DEL)