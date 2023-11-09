from numpy import array, float32, ndarray
from openai import OpenAI

from .base import BaseEmbbeder


class OpenAIEmbedder(BaseEmbbeder):
    dimension: int
    model: str

    def __init__(self, openai: OpenAI, model: str) -> None:
        """Create an embedder based on OpenAI's API.

        Args:
            openai (OpenAI): OpenAI instance.
            model (str): Model name.
        """
        self.openai = openai
        self.model = model
        res = self.openai.embeddings.create(
            input="Hello World!", model=model, encoding_format="float"
        )
        self.dimension = len(res.data[0].embedding)

    def get_dimension(self) -> int:
        return self.dimension

    def embed(self, text: str) -> ndarray:
        res = self.openai.embeddings.create(
            input=text, model=self.model, encoding_format="float"
        )
        return array(res.data[0].embedding, dtype=float32)
