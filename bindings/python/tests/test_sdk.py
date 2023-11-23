from typing import List

import numpy as np
import pytest

from pgvecto_rs.sdk import Filter, PGVectoRs, Record, filters
from tests import (
    EXPECTED_NEG_COS_DIS,
    EXPECTED_NEG_DOT_PROD_DIS,
    EXPECTED_SQRT_EUCLID_DIS,
    OP_NEG_COS_DIS,
    OP_NEG_DOT_PROD_DIS,
    OP_SQRT_EUCLID_DIS,
    URL,
    VECTORS,
)

URL = URL.replace("postgresql", "postgresql+psycopg")
MockTexts = {
    "text0": VECTORS[0],
    "text1": VECTORS[1],
    "text2": VECTORS[2],
}


class MockEmbedder:
    def embed(self, text: str) -> np.ndarray:
        if isinstance(MockTexts[text], list):
            return np.array(MockTexts[text], dtype=np.float32)
        return MockTexts[text]


@pytest.fixture(scope="module")
def client():
    client = PGVectoRs(db_url=URL, collection_name="empty", dimension=3, recreate=True)
    records1 = [Record.from_text(t, v, {"src": "src1"}) for t, v in MockTexts.items()]
    records2 = [Record.from_text(t, v, {"src": "src2"}) for t, v in MockTexts.items()]
    client.insert(records1)
    client.insert(records2)
    return client


filter_src1 = filters.meta_contains({"src": "src1"})
filter_src2: Filter = lambda r: r.meta.contains({"src": "src2"})


@pytest.mark.parametrize("filter", [filter_src1, filter_src2])
@pytest.mark.parametrize(
    ("dis_op", "dis_oprand", "dis_expected"),
    zip(
        ["<->", "<#>", "<=>"],
        [OP_SQRT_EUCLID_DIS, OP_NEG_DOT_PROD_DIS, OP_NEG_COS_DIS],
        [EXPECTED_SQRT_EUCLID_DIS, EXPECTED_NEG_DOT_PROD_DIS, EXPECTED_NEG_COS_DIS],
    ),
)
def test_search_filter_and_op(
    client: PGVectoRs,
    filter: Filter,
    dis_op: str,
    dis_oprand: List[float],
    dis_expected: List[float],
):
    for rec, dis in client.search(dis_oprand, dis_op, top_k=99, filter=filter):
        cnt = None
        for i in range(len(VECTORS)):
            if np.allclose(rec.embedding, VECTORS[i]):
                cnt = i
                break
        assert np.allclose(dis, dis_expected[cnt])


@pytest.mark.parametrize(
    ("dis_op", "dis_oprand", "dis_expected"),
    zip(
        ["<->", "<#>", "<=>"],
        [OP_SQRT_EUCLID_DIS, OP_NEG_DOT_PROD_DIS, OP_NEG_COS_DIS],
        [EXPECTED_SQRT_EUCLID_DIS, EXPECTED_NEG_DOT_PROD_DIS, EXPECTED_NEG_COS_DIS],
    ),
)
def test_search_order_and_limit(
    client: PGVectoRs,
    dis_op: str,
    dis_oprand: List[float],
    dis_expected: List[float],
):
    dis_expected = dis_expected.copy()
    dis_expected.sort()
    for i, (_rec, dis) in enumerate(client.search(dis_oprand, dis_op, top_k=4)):
        assert np.allclose(dis, dis_expected[i // 2])
