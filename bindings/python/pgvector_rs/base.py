import numpy as np
from functools import wraps


def _ignore_none(func):
    @wraps(func)
    def _func(value):
        return value if value is None else func(value)
    return _func


def _ignore_ndarray(func):
    @wraps(func)
    def _func(value):
        return value if isinstance(value, np.ndarray) else func(value)
    return _func


def _ndarray_valiadtor(func):
    @wraps(func)
    def _func(value):
        if isinstance(value, np.ndarray):
            if value.ndim != 1:
                raise ValueError('ndarray must be 1D for vector')
            if not np.issubdtype(value.dtype, np.float) \
                    or not np.issubdtype(value.dtype, np.integer):
                raise ValueError(
                    'ndarray data type must be numeric for vector'
                )
            value = value.tolist()
        return func(value)
    return _func


@_ignore_none
@_ndarray_valiadtor
def serilize(value):
    return '[' + ','.join([str(float(v)) for v in value]) + ']'


@_ignore_none
@_ignore_ndarray
def deserilize(value):
    return np.array(value[1:-1].split(','), dtype=np.float32)
