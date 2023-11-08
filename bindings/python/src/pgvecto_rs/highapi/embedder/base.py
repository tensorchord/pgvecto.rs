from numpy import ndarray, array
from abc import ABC, abstractmethod


class BaseEmbbeder(ABC):
    @abstractmethod
    def get_dimension(self) -> int:
        pass

    @abstractmethod
    def embed(self, text: str) -> ndarray:
        pass
