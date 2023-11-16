from typing import List, Optional, Type, Union
from uuid import UUID, uuid4

from numpy import array, float32, ndarray
from sqlalchemy.orm import DeclarativeBase, Mapped


class RecordORM(DeclarativeBase):
    __tablename__: str
    id: Mapped[UUID]
    text: Mapped[str]
    meta: Mapped[dict]
    embedding: Mapped[ndarray]


RecordORMType = Type[RecordORM]


class Record:
    id: UUID
    text: str
    meta: dict
    embedding: ndarray

    def __init__(self, id: UUID, text: str, meta: dict, embedding: ndarray):
        self.id = id
        self.text = text
        self.meta = meta
        self.embedding = embedding

    def __repr__(self) -> str:
        return f"""============= Record =============
[id]       : {self.id}
[text]     : {self.text}
[meta]     : {self.meta}
[embedding]: {self.embedding}
========== End of Record ========="""

    @classmethod
    def from_orm(cls, orm: RecordORM):
        return cls(orm.id, orm.text, orm.meta, orm.embedding)

    @classmethod
    def from_text(
        cls,
        text: str,
        embedding: Union[ndarray, List[float]],
        meta: Optional[dict] = None,
    ):
        if isinstance(embedding, list):
            embedding = array(embedding, dtype=float32)
        return cls(uuid4(), text, meta or {}, embedding)
