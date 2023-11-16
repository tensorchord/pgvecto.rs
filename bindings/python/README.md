# Python bindings for pgvecto.rs

[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm.fming.dev)

## Usage

Install from PyPI:
```bash
pip install pgvecto_rs
```

See the [usage of SDK](#sdk)

Or use it as an extension of postgres clients:
- [SQLAlchemy](#sqlalchemy)
- [psycopg3](#psycopg3)

### SDK

Our SDK is designed to use the pgvecto.rs out-of-box. You can exploit the power of pgvecto.rs to do similarity search or retrieve with filters, without writing any SQL code.

Install dependencies:
```bash
pip install "pgvecto_rs[sdk]"
```

A minimal example:

```Python
from pgvecto_rs.sdk import PGVectoRs, Record

# Create a client
client = PGVectoRs(
    db_url="postgresql+psycopg://postgres:mysecretpassword@localhost:5432/postgres",
    table_name="example",
    dimension=3,
)

try:
    # Add some records
    client.add_records(
        [
            Record.from_text("hello 1", [1, 2, 3]),
            Record.from_text("hello 2", [1, 2, 4]),
        ]
    )

    # Search with default operator (sqrt_euclid).
    # The results is sorted by distance
    for rec, dis in client.search([1, 2, 5]):
        print(rec.text)
        print(dis)
finally:
    # Clean up (i.e. drop the table)
    client.drop()
```

Output:
```
hello 2
1.0
hello 1
4.0
```

See [examples/sdk_example.py](examples/sdk_example.py) and [tests/test_sdk.py](tests/test_sdk.py) for more examples.


### SQLAlchemy

Install dependencies:
```bash
pip install "pgvecto_rs[sqlalchemy]"
```

Then write your code. See [examples/sqlalchemy_example.py](examples/sqlalchemy_example.py) and [tests/test_sqlalchemy.py](tests/test_sqlalchemy.py) for example.

All the operators include:
- `squared_euclidean_distance`
- `negative_dot_product_distance`
- `negative_cosine_distance`

### psycopg3

Install dependencies:
```bash
pip install "pgvecto_rs[psycopg3]"
```

Then write your code. See [examples/psycopg_example.py](examples/psycopg_example.py) and [tests/test_psycopg.py](tests/test_psycopg.py) for example.

Known issue: 
- Can not check the length of an vector when inserting it into a table. See: [#96](https://github.com/tensorchord/pgvecto.rs/issues/96).

## Development

This package is managed by [PDM](https://pdm.fming.dev).

Set up things:
```bash
pdm venv create
pdm use # select the venv inside the project path
pdm sync
```

Run lint:
```bash
pdm run format
pdm run fix
pdm run check
```

Run test in current environment:
```bash
pdm run test
```


## Test

[Tox](https://tox.wiki) is used to test the package locally.

Run test in all environment:
```bash
tox run
```