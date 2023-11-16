import os

import numpy as np
import toml

PORT = os.getenv("DB_PORT", "5432")
HOST = os.getenv("DB_HOST", "localhost")
USER = os.getenv("DB_USER", "postgres")
PASS = os.getenv("DB_PASS", "mysecretpassword")
DB_NAME = os.getenv("DB_NAME", "postgres")

# Run tests with shell:
#   DB_HOST=localhost DB_USER=postgres DB_PASS=password DB_NAME=postgres python3 -m pytest bindings/python/tests/
URL = f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DB_NAME}"


# ==== test_create_index ====

TOML_SETTINGS = {
    "flat": toml.dumps(
        {
            "capacity": 2097152,
            "algorithm": {"flat": {}},
        },
    ),
    "hnsw": toml.dumps(
        {
            "capacity": 2097152,
            "algorithm": {"hnsw": {}},
        },
    ),
}

# ==== test_invalid_insert ====
INVALID_VECTORS = [
    [1, 2, 3, 4],
    [
        1,
    ],
    [[1, 2], [3, 4], [5, 6]],
    ["123.", "123", "a"],
    np.array([1, 2, 3, 4]),
    np.array([1, "3", 3]),
    np.zeros(shape=(1, 2)),
]

# =================================
# Semetic search tests
# =================================
VECTORS = [
    [1, 2, 3],
    [0.0, -45, 2.34],
    np.ones(shape=(3)),
]
OP_SQRT_EUCLID_DIS = [0, 0, 0]
EXPECTED_SQRT_EUCLID_DIS = [14.0, 2030.4756, 3.0]
OP_NEG_DOT_PROD_DIS = [1, 2, 4]
EXPECTED_NEG_DOT_PROD_DIS = [-17.0, 80.64, -7.0]
OP_NEG_COS_DIS = [3, 2, 1]
EXPECTED_NEG_COS_DIS = [-0.7142857, 0.5199225, -0.92582005]

# ==== test_delete ====
LEN_AFT_DEL = 2

__all__ = [
    "URL",
    "TOML_SETTINGS",
    "INVALID_VECTORS",
    "VECTORS",
    "EXPECTED_SQRT_EUCLID_DIS",
    "EXPECTED_NEG_DOT_PROD_DIS",
    "EXPECTED_NEG_COS_DIS",
    "LEN_AFT_DEL",
]
