import sqlalchemy.types as types
from pgvector_rs.base import serilize, deserilize


class Vector(types.UserDefinedType):
    cache_ok = True

    def __init__(self, dim):
        self.dim = dim

    def get_col_spec(self, **kw):
        if self.dim is None or self.dim <= 0:
            return "VECTOR"
        return "VECTOR(%s)" % self.dim

    def bind_processor(self, dialect):
        return serilize

    def result_processor(self, dialect, coltype):
        return deserilize
