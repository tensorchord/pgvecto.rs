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
    client = PGVectoRs(
        db_url=URL,
        table_name="empty",
        dimension=3,
        new_table=True,
    )
    try:
        for t, v in mockTexts.items():
            client.add_record(Record.from_text(t, {"src": "src1"}, v))
        for t, v in mockTexts.items():
            client.add_record(Record.from_text(t, {"src": "src2"}, v))
        yield client
    finally:
        client.drop()


def test_client_from_records():
    try:
        client = PGVectoRs.from_records(
            [Record.from_text(t, None, e) for t, e in mockTexts.items()],
            db_url=URL,
            table_name="from_records",
            dimension=3,
        )
        results = client.search([0, 0, 0], "<#>", 99, order_by_dis=False)
        assert len(results) == 3
        for i in range(3):
            assert results[i][0].text == f"text{i}"
            assert np.allclose(results[i][0].embedding, mockTexts[f"text{i}"])
    finally:
        client.drop()


filter_src1 = filters.meta_contains({"src": "src1"})
filter_src2: Filter = lambda r: r.meta.contains({"src": "src2"})


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
    for i, (rec, dis) in enumerate(
        client.search(dis_oprand, dis_op, limit=99, filter=filter, order_by_dis=False)
    ):
        assert np.allclose(dis, dis_expected[i])


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
    for i, (rec, dis) in enumerate(client.search(dis_oprand, dis_op, limit=4)):
        assert np.allclose(dis, dis_expected[i // 2])
