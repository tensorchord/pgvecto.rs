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
mockTexts = {
    "text0": VECTORS[0],
    "text1": VECTORS[1],
    "text2": VECTORS[2],
}


class MockEmbedder:
    def embed(self, text: str) -> np.ndarray:
        if isinstance(mockTexts[text], list):
            return np.array(mockTexts[text], dtype=np.float32)
        return mockTexts[text]


@pytest.fixture(scope="module")
def client():
    client = PGVectoRs(db_url=URL, collection_name="empty", dimension=3)
    try:
        records1 = [
            Record.from_text({"text": t, "src": "src1"}, v)
            for t, v in mockTexts.items()
        ]
        records2 = [
            Record.from_text({"text": t, "src": "src2"}, v)
            for t, v in mockTexts.items()
        ]
        client.upsert(records1)
        client.upsert(records2)
        yield client
    finally:
        client.drop()


filter_src1 = filters.document_contains({"src": "src1"})
filter_src2: Filter = lambda r: r.document.contains({"src": "src2"})


@pytest.mark.parametrize("filter", [filter_src1, filter_src2])
@pytest.mark.parametrize(
    "dis_op, dis_oprand, dis_expected",
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
    "dis_op, dis_oprand, dis_expected",
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
    for i, (rec, dis) in enumerate(client.search(dis_oprand, dis_op, top_k=4)):
        assert np.allclose(dis, dis_expected[i // 2])
