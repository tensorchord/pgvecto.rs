import numpy as np
from functools import wraps


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


def valiadte_ndarray(func):
    @wraps(func)
    def _func(value, *args, **kwargs):
        if isinstance(value, np.ndarray):
            if value.ndim != 1:
                raise ValueError('ndarray must be 1D for vector')
            if not np.issubdtype(value.dtype, np.number):
                raise ValueError(
                    'ndarray data type must be numeric for vector'
                )
            value = value.tolist()
        return func(value, *args, **kwargs)
    return _func


def serilize(value):
    '''
    define your own decorators or use predefined decorators to add check or data processing logic
    '''
    return '[' + ','.join([str(float(v)) for v in value]) + ']'


def deserilize(value):
    '''
    define your own decorators or use predefined decorators to add check or data processing logic
    '''
    return np.array(value[1:-1].split(','), dtype=np.float32)
