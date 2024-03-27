from numpy import ndarray
from psycopg import Connection, ProgrammingError
from psycopg.adapt import Dumper, Loader
from psycopg.pq import Format
from psycopg.types import TypeInfo

from pgvecto_rs.types import SparseVector
from pgvecto_rs.utils.serializer import (
    from_db_binary,
    from_db_binary_sparse,
    from_db_str,
    to_db_binary,
    to_db_binary_sparse,
    to_db_str,
)

__all__ = ["register_vector"]


class VectorTextDumper(Dumper):
    format = Format.TEXT

    def dump(self, obj):
        return to_db_str(obj).encode("utf8")


class VectorBinaryDumper(Dumper):
    format = Format.BINARY

    def dump(self, obj):
        return to_db_binary(obj)


class SparseVectorTextDumper(Dumper):
    format = Format.BINARY

    def dump(self, obj):
        return to_db_binary_sparse(obj)


class VectorTextLoader(Loader):
    format = Format.TEXT

    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return from_db_str(data.decode("utf8"))


class VectorBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data):
        return from_db_binary(data)


class SparseVectorBinaryLoader(Loader):
    format = Format.BINARY

    def load(self, data):
        return from_db_binary_sparse(data)


def register_vector(context: Connection):
    info = TypeInfo.fetch(context, "vector")
    register_vector_info(context, info)
    info = TypeInfo.fetch(context, "svector")
    register_svector_info(context, info)


async def register_vector_async(context: Connection):
    info = await TypeInfo.fetch(context, "vector")
    register_vector_info(context, info)
    info = await TypeInfo.fetch(context, "svector")
    register_svector_info(context, info)


def register_vector_info(context: Connection, info: TypeInfo):
    if info is None:
        raise ProgrammingError(info="vector type not found in the database")
    info.register(context)

    # Dumper for text and binary
    vector_text_dumper = type("", (VectorTextDumper,), {"oid": info.oid})
    vector_binary_dumper = type("", (VectorBinaryDumper,), {"oid": info.oid})

    # Register the dumper and loader
    adapters = context.adapters
    adapters.register_dumper(list, vector_text_dumper)
    adapters.register_dumper(ndarray, vector_text_dumper)
    adapters.register_dumper(list, vector_binary_dumper)
    adapters.register_dumper(ndarray, vector_binary_dumper)
    adapters.register_loader(info.oid, VectorTextLoader)
    adapters.register_loader(info.oid, VectorBinaryLoader)


def register_svector_info(context: Connection, info: TypeInfo):
    if info is None:
        raise ProgrammingError(info="svector type not found in the database")
    info.register(context)

    # Dumper for binary
    svector_binary_dumper = type("", (SparseVectorTextDumper,), {"oid": info.oid})

    # Register the dumper and loader
    adapters = context.adapters
    adapters.register_dumper(SparseVector, svector_binary_dumper)
    adapters.register_loader(info.oid, SparseVectorBinaryLoader)
