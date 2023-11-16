from typing import Callable, Type

from sqlalchemy import ColumnElement

from pgvecto_rs.sdk.record import RecordORMType

Filter = Type[Callable[[RecordORMType], ColumnElement[bool]]]
FilterInput = RecordORMType
FilterOutput = ColumnElement[bool]


def meta_contains(meta_contains: dict) -> Filter:
    return lambda r: r.meta.contains(meta_contains)
