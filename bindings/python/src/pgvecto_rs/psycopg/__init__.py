from psycopg import Connection, ProgrammingError
from psycopg.adapt import Loader, Dumper
from psycopg.pq import Format
from psycopg.types import TypeInfo
from numpy import ndarray
from ..utils.serializer import from_db_str, to_db_str
from typing import List, Union

__all__ = ['register_vector']


class VectorDumper(Dumper):

    format = Format.TEXT

    def dump(self, obj):
        return to_db_str(obj).encode("utf8")




class VectorLoader(Loader):

    format = Format.TEXT

    def load(self, data):
        if isinstance(data, memoryview):
            data = bytes(data)
        return from_db_str(data.decode("utf8"))




def register_vector(context: Connection):
    info = TypeInfo.fetch(context, 'vector')
    register_vector_info(context, info)


async def register_vector_async(context: Connection):
    info = await TypeInfo.fetch(context, 'vector')
    register_vector_info(context, info)


def register_vector_info(context: Connection, info: TypeInfo):
    if info is None:
        raise ProgrammingError('vector type not found in the database')
    info.register(context)

    # add oid to anonymous class for set_types
    text_dumper = type('', (VectorDumper,), {'oid': info.oid})

    adapters = context.adapters
    adapters.register_dumper(list, text_dumper)
    adapters.register_dumper(ndarray, text_dumper)
    adapters.register_loader(info.oid, VectorLoader)