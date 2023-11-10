from typing import Callable, Type

from sqlalchemy import ColumnElement

from ..record import RecordORMType

FilterFunc = Type[Callable[[RecordORMType], ColumnElement[bool]]]
