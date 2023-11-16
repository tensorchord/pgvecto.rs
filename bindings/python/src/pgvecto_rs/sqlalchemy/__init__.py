import sqlalchemy.types as types

from pgvecto_rs.utils import serializer


class Vector(types.UserDefinedType):
    cache_ok = True

    def __init__(self, dim):
        if dim < 0:
            raise ValueError("negative dim is not allowed")
        self.dim = dim

    def get_col_spec(self, **kw):
        if self.dim is None or self.dim == 0:
            return "VECTOR"
        return "VECTOR({})".format(self.dim)

    def bind_processor(self, dialect):
        def _processor(value):
            return serializer.to_db_str(value, self.dim)

        return _processor

    def result_processor(self, dialect, coltype):
        def _processor(value):
            return serializer.from_db_str(value)

        return _processor

    class comparator_factory(types.UserDefinedType.Comparator):
        def squared_euclidean_distance(self, other):
            return self.op("<->", return_type=types.Float)(other)

        def negative_dot_product_distance(self, other):
            return self.op("<#>", return_type=types.Float)(other)

        def negative_cosine_distance(self, other):
            return self.op("<=>", return_type=types.Float)(other)
