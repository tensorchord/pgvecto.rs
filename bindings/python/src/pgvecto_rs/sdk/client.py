from typing import List, Literal, Optional, Tuple, Type, Union
from uuid import UUID

from numpy import ndarray
from sqlalchemy import (
    ColumnElement,
    Float,
    String,
    create_engine,
    delete,
    insert,
    select,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.orm.session import Session

from pgvecto_rs.sqlalchemy import Vector

from .filters import Filter
from .record import Record, RecordORM, RecordORMType


class PGVectoRs:
    _engine: Engine
    _table: Type[RecordORM]
    dimension: int

    def __init__(
        self,
        db_url: str,
        table_name: str,
        dimension: int,
        new_table: bool = False,
    ) -> None:
        """Connect to an existing table or create a new one.

        Args:
            db_url (str): url to the database.
            table_name (str): name of the table.
            dimension (int): dimension of the embeddings.
            new_table (bool, optional): Defaults to False.

        Raises:
            ValueError: When the dimension of the embedder does not match the given dimension.
        """

        class _Table(RecordORM):
            __tablename__ = f"table_{table_name}"
            id: Mapped[UUID] = mapped_column(
                postgresql.UUID(as_uuid=True), primary_key=True
            )
            text: Mapped[str] = mapped_column(String)
            meta: Mapped[dict] = mapped_column(postgresql.JSONB)
            embedding: Mapped[ndarray] = mapped_column(Vector(dimension))

        self._engine = create_engine(db_url)
        with Session(self._engine) as session:
            session.execute(text("CREATE EXTENSION IF NOT EXISTS vectors"))
            session.commit()
        self._table = _Table
        if new_table:
            self._table.__table__.create(self._engine, checkfirst=False)
        self.dimension = dimension

    @classmethod
    def from_records(
        cls,
        records: List[Record],
        db_url: str,
        table_name: str,
        dimension: int,
    ):
        client = cls(db_url, table_name, dimension, True)
        try:
            for record in records:
                client.add_record(record)
        except Exception as e:
            client.drop()
            raise e
        return client

    def add_record(self, record: Record) -> None:
        with Session(self._engine) as session:
            session.execute(
                insert(self._table).values(
                    id=record.id,
                    text=record.text,
                    meta=record.meta,
                    embedding=record.embedding,
                )
            )
            session.commit()

    def search(
        self,
        embedding: Union[ndarray, List[float]],
        distance_op: Literal["<->", "<=>", "<#>"] = "<->",
        limit: int = 10,
        filter: Optional[Filter] = None,
        order_by_dis: bool = True,
    ) -> List[Tuple[Record, float]]:
        """Search for the nearest records.

        Args:
            embedding : Target embedding.
            distance_op : Distance op.
            limit : Max records to return. Defaults to 10.
            filter : Read our document. Defaults to None.
            order_by_dis : Order by distance. Defaults to True.

        Returns:
            List of records and coresponding distances.

        """
        with Session(self._engine) as session:
            stmt = select(
                self._table,
                self._table.embedding.op(distance_op, return_type=Float)(
                    embedding
                ).label("distance"),
            ).limit(limit)
            if order_by_dis:
                stmt = stmt.order_by("distance")
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
