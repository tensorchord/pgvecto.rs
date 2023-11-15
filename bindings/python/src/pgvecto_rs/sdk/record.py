from typing import List, Optional, Type, Union
from uuid import UUID, uuid4

from numpy import array, float32, ndarray
from sqlalchemy.orm import DeclarativeBase, Mapped


class RecordORM(DeclarativeBase):
    __tablename__: str
    id: Mapped[UUID]
    document: Mapped[dict]
    embedding: Mapped[ndarray]


RecordORMType = Type[RecordORM]


class Record:
    id: UUID
    document: dict
    embedding: ndarray

    def __init__(self, id: UUID, document: dict, embedding: ndarray):
        self.id = id
        self.document = document
        self.embedding = embedding

    def __repr__(self) -> str:
        return f"""============= Record =============
[id]       : {self.id}
[document]     : {self.document}
[embedding]: {self.embedding}
========== End of Record ========="""

    @classmethod
    def from_orm(cls, orm: RecordORM):
        return cls(orm.id, orm.document, orm.embedding)

    @classmethod
    def from_text(
        cls, document: Optional[dict], embedding: Union[ndarray, List[float]]
    ):
        if isinstance(embedding, list):
            embedding = array(embedding, dtype=float32)
        return cls(uuid4(), document or {}, embedding)
