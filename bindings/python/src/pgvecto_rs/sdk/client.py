from typing import List, Literal, Optional, Tuple, Type, Union
from uuid import UUID

from numpy import ndarray
from sqlalchemy import ColumnElement, Float, create_engine, delete, insert, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm.session import Session
from sqlalchemy.types import String

from pgvecto_rs.sdk.filters import Filter
from pgvecto_rs.sdk.record import Record, RecordORM, RecordORMType
from pgvecto_rs.sqlalchemy import Vector


class PGVectoRs:
    _engine: Engine
    _table: Type[RecordORM]
    dimension: int

    def __init__(
        self, db_url: str, collection_name: str, dimension: int, recreate: bool = False
    ) -> None:
        """Connect to an existing table or create a new empty one.
        If the `recreate=True`, the table will be dropped if it exists.

        Args:
        ----
            db_url (str): url to the database.
            table_name (str): name of the table.
            dimension (int): dimension of the embeddings.
            recreate (bool): drop the table if it exists. Defaults to False.
        """

        class _Table(RecordORM):
            __tablename__ = f"collection_{collection_name}"
            __table_args__ = {"extend_existing": True}  # noqa: RUF012
            id: Mapped[UUID] = mapped_column(
                postgresql.UUID(as_uuid=True),
                primary_key=True,
            )
            text: Mapped[str] = mapped_column(String)
            meta: Mapped[dict] = mapped_column(postgresql.JSONB)
            embedding: Mapped[ndarray] = mapped_column(Vector(dimension))

        self._engine = create_engine(db_url)
        with Session(self._engine) as session:
            session.execute(text("CREATE EXTENSION IF NOT EXISTS vectors"))
            if recreate:
                session.execute(text(f"DROP TABLE IF EXISTS {_Table.__tablename__}"))
            session.commit()
        self._table = _Table
        self._table.__table__.create(self._engine, checkfirst=True)
        self.dimension = dimension

    def insert(self, records: List[Record]) -> None:
        with Session(self._engine) as session:
            for record in records:
                session.execute(
                    insert(self._table).values(
                        id=record.id,
                        text=record.text,
                        meta=record.meta,
                        embedding=record.embedding,
                    ),
                )
            session.commit()

    def search(
        self,
        embedding: Union[ndarray, List[float]],
        distance_op: Literal["<->", "<=>", "<#>"] = "<->",
        top_k: int = 4,
        filter: Optional[Filter] = None,
    ) -> List[Tuple[Record, float]]:
        """Search for the nearest records.

        Args:
        ----
            embedding : Target embedding.
            distance_op : Distance op.
            top_k : Max records to return. Defaults to 4.
            filter : Read our document. Defaults to None.
            order_by_dis : Order by distance. Defaults to True.

        Returns:
        -------
            List of records and coresponding distances.

        """
        with Session(self._engine) as session:
            stmt = (
                select(
                    self._table,
                    self._table.embedding.op(distance_op, return_type=Float)(
                        embedding,
                    ).label("distance"),
                )
                .limit(top_k)
                .order_by("distance")
            )
            if filter is not None:
                stmt = stmt.where(filter(self._table))
            res = session.execute(stmt)
            return [(Record.from_orm(row[0]), row[1]) for row in res]

    # ================ Delete ================
    def delete(self, filter: Filter) -> None:
        with Session(self._engine) as session:
            session.execute(delete(self._table).where(filter(self._table)))
            session.commit()

    def delete_all(self) -> None:
        with Session(self._engine) as session:
            session.execute(delete(self._table))
            session.commit()

    def delete_by_ids(self, ids: List[UUID]) -> None:
        def filter(record: RecordORMType) -> ColumnElement[bool]:
            return record.id.in_(ids)

        with Session(self._engine) as session:
            session.execute(delete(self._table).where(filter(self._table)))
            session.commit()

    # ================ Drop ================
    def drop(self) -> None:
        """Drop the table which the client is connected to."""
        self._table.__table__.drop(self._engine)
