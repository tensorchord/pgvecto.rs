from typing import Callable, Type

from sqlalchemy import ColumnElement

from pgvecto_rs.sdk.record import RecordORMType

Filter = Type[Callable[[RecordORMType], ColumnElement[bool]]]
FilterInput = RecordORMType
FilterOutput = ColumnElement[bool]


def document_contains(document_contains: dict) -> Filter:
    return lambda r: r.document.contains(document_contains)


__all__ = ["Filter", "FilterInput", "FilterOutput", "document_contains"]
