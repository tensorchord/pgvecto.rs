from functools import wraps

import numpy as np

from pgvecto_rs.errors import (
    BuiltinListTypeError,
    NDArrayDimensionError,
    NDArrayDtypeError,
    SparseVectorTypeError,
)
from pgvecto_rs.types import SparseVector


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


def validate_sparse_vector(func):
    """Validate sparse vector data type"""

    @wraps(func)
    def _func(vector: SparseVector, *args, **kwargs):
        if isinstance(vector, SparseVector):
            (dims, indices, values) = vector
            if not isinstance(dims, int):
                raise SparseVectorTypeError("dims", [int], type(dims))
            if not isinstance(indices, (np.ndarray, list)):
                raise SparseVectorTypeError(
                    "indices", [list, np.ndarray], type(indices)
                )
            if isinstance(indices, np.ndarray):
                if indices.ndim != 1:
                    raise NDArrayDimensionError(indices.ndim)
                if not np.issubdtype(indices.dtype, np.integer):
                    raise SparseVectorTypeError(
                        "element of indices", [int, np.integer], indices.dtype
                    )
            if isinstance(indices, list):
                for x in indices:
                    if not isinstance(x, int):
                        raise SparseVectorTypeError(
                            "element of indices", [int, np.integer], type(x)
                        )
                indices = np.array(indices, dtype=np.uint32)
            if not isinstance(values, (np.ndarray, list)):
                raise SparseVectorTypeError("values", [list, np.ndarray], type(values))
            if isinstance(values, np.ndarray):
                if values.ndim != 1:
                    raise NDArrayDimensionError(values.ndim)
                if not np.issubdtype(values.dtype, np.number):
                    raise SparseVectorTypeError(
                        "element of values", [int, float, np.number], values.dtype
                    )
            if isinstance(values, list):
                for x in values:
                    if not isinstance(x, (int, float)):
                        raise SparseVectorTypeError(
                            "element of values", [int, float, np.number], type(x)
                        )
                values = np.array(values, dtype=np.float32)

        return func(SparseVector(dims, indices, values), *args, **kwargs)

    return _func
