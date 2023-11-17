from functools import wraps

import numpy as np

from pgvecto_rs.errors import (
    BuiltinListTypeError,
    NDArrayDimensionError,
    NDArrayDtypeError,
)


def ignore_none(func):
    @wraps(func)
    def _func(value, *args, **kwargs):
        return value if value is None else func(value, *args, **kwargs)

    return _func


def ignore_ndarray(func):
    @wraps(func)
    def _func(value, *args, **kwargs):
        return value if isinstance(value, np.ndarray) else func(value, *args, **kwargs)

    return _func


def validate_ndarray(func):
    """Validate ndarray data type for vector"""

    @wraps(func)
    def _func(value: np.ndarray, *args, **kwargs):
        if isinstance(value, np.ndarray):
            if value.ndim != 1:
                raise NDArrayDimensionError(value.ndim)
            if not np.issubdtype(value.dtype, np.number):
                raise NDArrayDtypeError(value.dtype)
        return func(value, *args, **kwargs)

    return _func


def validate_builtin_list(func):
    """Validate list data type for vector and convert to ndarray"""

    @wraps(func)
    def _func(value: list, *args, **kwargs):
        if isinstance(value, list):
            if not all(isinstance(x, (int, float)) for x in value):
                raise BuiltinListTypeError()
            value = np.array(value, dtype=np.float32)
        return func(value, *args, **kwargs)

    return _func
