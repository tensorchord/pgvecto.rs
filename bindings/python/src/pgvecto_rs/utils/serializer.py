from struct import pack, unpack
from typing import Optional

import numpy as np

from . import decorators


@decorators.ignore_none
@decorators.ignore_ndarray  # could be ndarray if already cast by lower-level driver
def from_db_str(value):
    return np.array(value[1:-1].split(","), dtype=np.float32)


@decorators.ignore_none
def from_db_binary(value: bytes) -> np.ndarray:
    dim = unpack("<H", value[:2])[0]
    return np.frombuffer(value, dtype="<f", count=dim, offset=2).astype(np.float32)


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

    value = np.asarray(value, dtype="<f")

    if value.ndim != 1:
        raise ValueError("expected 1d array, not %d" % value.ndim)
    dims: bytes = pack(
        "<H",
        value.shape[0],
    )
    return dims + value.tobytes()
