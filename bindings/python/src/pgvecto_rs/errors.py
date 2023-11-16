import numpy as np


class VectorError(ValueError):
    pass


class NDArrayDimensionError(VectorError):
    def __init__(self, dim: int) -> None:
        super().__init__(f"ndarray must be 1D for vector, got {dim}D")


class NDArrayDtypeError(VectorError):
    def __init__(self, dtype: np.dtype) -> None:
        super().__init__(f"ndarray data type must be numeric for vector, got {dtype}")


class BuiltinListTypeError(VectorError):
    def __init__(self) -> None:
        super().__init__("list data type must be numeric for vector")


class VectorDimensionError(VectorError):
    def __init__(self, dim: int) -> None:
        super().__init__(f"vector dimension must be > 0, got {dim}")
