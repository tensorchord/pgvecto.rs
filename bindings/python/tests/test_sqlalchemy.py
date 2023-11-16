import numpy as np
import pytest
from sqlalchemy import Index, Integer, create_engine, delete, insert, select, text
from sqlalchemy.exc import StatementError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from pgvecto_rs.sqlalchemy import Vector
from tests import (
    EXPECTED_NEG_COS_DIS,
    EXPECTED_NEG_DOT_PROD_DIS,
    EXPECTED_SQRT_EUCLID_DIS,
    INVALID_VECTORS,
    LEN_AFT_DEL,
    OP_NEG_COS_DIS,
    OP_NEG_DOT_PROD_DIS,
    OP_SQRT_EUCLID_DIS,
    TOML_SETTINGS,
    URL,
    VECTORS,
)


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "tb_test_item"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    embedding: Mapped[np.ndarray] = mapped_column(Vector(3))

    def __repr__(self) -> str:
        return f"{self.embedding}"


@pytest.fixture(scope="module")
def session():
    """Connect to the test db pointed by the URL. Can check more details
    in `tests/__init__.py`
    """
    engine = create_engine(URL.replace("postgresql", "postgresql+psycopg"))

    # ensure that we have installed pgvector.rs extension
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS vectors"))
        conn.execute(text("DROP TABLE IF EXISTS tb_test_item"))
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


@pytest.mark.parametrize(("index_name", "index_setting"), TOML_SETTINGS.items())
def test_create_index(session: Session, index_name: str, index_setting: str):
    index = Index(
        index_name,
        Document.embedding,
        postgresql_using="vectors",
        postgresql_with={"options": f"$${index_setting}$$"},
        postgresql_ops={"embedding": "l2_ops"},
    )
    index.create(session.bind)
    session.commit()


@pytest.mark.parametrize(("i", "e"), enumerate(INVALID_VECTORS))
def test_invalid_insert(session: Session, i: int, e: np.ndarray):
    try:
        session.execute(insert(Document).values(id=i, embedding=e))
    except StatementError:
        pass
    else:
        raise AssertionError(  # noqa: TRY003
            f"failed to raise invalid value error for {i}th vector {e}",
        )
    finally:
        session.rollback()


# =================================
# Semetic search tests
# =================================


def test_insert(session: Session):
    for stat in [
        insert(Document).values(id=i, embedding=e) for i, e in enumerate(VECTORS)
    ]:
        session.execute(stat)
    session.commit()
    for row in session.scalars(select(Document)):
        assert np.allclose(row.embedding, VECTORS[row.id], atol=1e-10)


def test_squared_euclidean_distance(session: Session):
    for row in session.execute(
        select(
            Document.id,
            Document.embedding.squared_euclidean_distance(OP_SQRT_EUCLID_DIS),
        ),
    ):
        (i, res) = row
        assert np.allclose(EXPECTED_SQRT_EUCLID_DIS[i], res, atol=1e-10)


def test_negative_dot_product_distance(session: Session):
    for row in session.execute(
        select(
            Document.id,
            Document.embedding.negative_dot_product_distance(OP_NEG_DOT_PROD_DIS),
        ),
    ):
        (i, res) = row
        assert np.allclose(EXPECTED_NEG_DOT_PROD_DIS[i], res, atol=1e-10)


def test_negative_cosine_distance(session: Session):
    for row in session.execute(
        select(
            Document.id, Document.embedding.negative_cosine_distance(OP_NEG_COS_DIS)
        ),
    ):
        (i, res) = row
        assert np.allclose(EXPECTED_NEG_COS_DIS[i], res, atol=1e-10)


# # =================================
# # Suffix functional tests
# # =================================


def test_delete(session: Session):
    session.execute(delete(Document).where(Document.embedding == VECTORS[0]))
    session.commit()
    res = session.execute(select(Document))
    assert len(list(res)) == LEN_AFT_DEL
