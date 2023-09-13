import sqlalchemy.types as types
from pgvector_rs.base import serilize, deserilize


class Vector(types.UserDefinedType):
    cache_ok = True

    def __init__(self, dim):
        self.dim = dim

    def get_col_spec(self, **kw):
        if self.dim is None or self.dim <= 0:
            return "VECTOR"
        return "VECTOR({})".format(self.dim)

    def bind_processor(self, dialect):
        def _processor(value):
            if len(value) != self.dim:
                raise ValueError("invalid dim for value: {}".format(value))
            return serilize(value)
        return _processor

    def result_processor(self, dialect, coltype):
        return deserilize
