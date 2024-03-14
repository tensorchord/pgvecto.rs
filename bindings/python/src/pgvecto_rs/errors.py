from typing import List

import numpy as np


class PGVectoRsError(ValueError):
    pass


class NDArrayDimensionError(PGVectoRsError):
    def __init__(self, dim: int) -> None:
        super().__init__(f"ndarray must be 1D for vector, got {dim}D")


class NDArrayDtypeError(PGVectoRsError):
    def __init__(self, dtype: np.dtype) -> None:
        super().__init__(f"ndarray data type must be numeric for vector, got {dtype}")


class BuiltinListTypeError(PGVectoRsError):
    def __init__(self) -> None:
        super().__init__("list data type must be numeric for vector")


class VectorDimensionError(PGVectoRsError):
    def __init__(self, dim: int) -> None:
        super().__init__(f"vector dimension must be > 0, got {dim}")


class SparseVectorTypeError(PGVectoRsError):
    def __init__(
        self, field: str, expected_type: List[type], actual_type: type
    ) -> None:
        super().__init__(
            f"{field} in SparseVector must be of type { ' or '.join(map(lambda t: t.__name__, expected_type))}, got {actual_type.__name__}"
        )


class SparseVectorElementTypeError(PGVectoRsError):
    def __init__(
        self, field: str, expected_type: List[type], actual_type: type
    ) -> None:
        super().__init__(
            f"elements of {field} in SparseVector must be of type { ' or '.join(map(lambda t: t.__name__, expected_type))}, got {actual_type.__name__}"
        )
