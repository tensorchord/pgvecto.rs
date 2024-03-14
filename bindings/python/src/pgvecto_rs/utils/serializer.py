from struct import pack, unpack
from typing import Optional

import numpy as np

from pgvecto_rs.types import SparseVector

from . import decorators


@decorators.ignore_none
@decorators.ignore_ndarray  # could be ndarray if already cast by lower-level driver
def from_db_str(value):
    return np.array(value[1:-1].split(","), dtype=np.float32)


@decorators.ignore_none
def from_db_binary(value: bytes) -> np.ndarray:
    # unpack as little-endian uint16, keep same endian with pgvecto.rs
    dim = unpack("<H", value[:2])[0]
    # start reading buffer from 3th byte (first 2 bytes are for dimension info)
    return np.frombuffer(value, dtype="<f", count=dim, offset=2).astype(np.float32)


@decorators.ignore_none
def from_db_binary_sparse(value: bytes) -> SparseVector:
    # unpack dims and length as little-endian uint32, keep same endian with pgvecto.rs
    dims = unpack("<I", value[:4])[0]
    length = unpack("<I", value[4:8])[0]
    bytes = value[8:]
    # unpack indices and values as little-endian uint32 and float32, keep same endian with pgvecto.rs
    indices = np.frombuffer(bytes, dtype="<I", count=length, offset=0).astype(np.uint32)
    bytes = bytes[4 * length :]
    values = np.frombuffer(bytes, dtype="<f", count=length, offset=0).astype(np.float32)
    return SparseVector(dims, indices, values)


@decorators.ignore_none
@decorators.validate_builtin_list
@decorators.validate_ndarray
def to_db_str(value: np.ndarray, dim: Optional[int] = None):
    if dim is not None and value.shape[0] != dim:
        raise ValueError("expected %d dimensions, not %d" % (dim, len(value)))

    return "[" + ",".join([str(float(v)) for v in value]) + "]"


@decorators.ignore_none
@decorators.validate_builtin_list
@decorators.validate_ndarray
def to_db_binary(value: np.ndarray, dim: Optional[int] = None) -> bytes:
    if dim is not None and value.shape[0] != dim:
        raise ValueError("expected %d dimensions, not %d" % (dim, len(value)))

        # cast to little-endian float32, keep same endian with pgvecto.rs
    value = np.asarray(value, dtype="<f")

    if value.ndim != 1:
        raise ValueError("expected 1d array, not %d" % value.ndim)
        # pack to little-endian uint16, keep same endian with pgvecto.rs
    dims: bytes = pack(
        "<H",
        value.shape[0],
    )
    return dims + value.tobytes()


@decorators.ignore_none
@decorators.validate_sparse_vector
def to_db_binary_sparse(value: SparseVector) -> bytes:
    (dims, indices, values) = value
    # convert indices to little-endian uint32
    indices = np.asarray(indices, dtype="<I")
    indices_len = indices.shape[0]
    indices_bytes = indices.tobytes()
    # convert values to little-endian float32
    values = np.asarray(values, dtype="<f")
    values_len = values.shape[0]
    values_bytes = values.tobytes()
    # check indices and values length is the same
    if indices_len != values_len:
        raise ValueError(
            "sparse vector expected indices length %d to match values length %d"
            % (indices_len, values_len)
        )
    return pack("<I", dims) + pack("<I", indices_len) + indices_bytes + values_bytes
