from abc import ABC, abstractmethod

from numpy import ndarray


class BaseEmbbeder(ABC):
    @abstractmethod
    def get_dimension(self) -> int:
        pass

    @abstractmethod
    def embed(self, text: str) -> ndarray:
        pass
