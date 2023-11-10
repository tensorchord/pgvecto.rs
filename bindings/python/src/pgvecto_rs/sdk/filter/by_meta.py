from sqlalchemy import ColumnElement

from ..record import RecordORMType
from .schema import FilterFunc


def filter_meta_contains(meta_contains: dict) -> FilterFunc:
    def _filter(r: RecordORMType) -> ColumnElement[bool]:
        return r.meta.contains(meta_contains)

    return _filter


def simpleFilter(r: RecordORMType) -> ColumnElement[bool]:
    return r.meta.contains({"foo": "bar"}) and r.text.startswith("title")
