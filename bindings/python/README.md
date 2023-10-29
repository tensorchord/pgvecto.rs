# Python bindings for pgvector.rs

[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm.fming.dev)

Currently supports [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy).

## Usage

Install from PyPI:
```bash
pip install pgvecto_rs
```

See the usage examples:
- [SQLAlchemy](#SQLAlchemy)
- [psycopg3](#psycopg3)

### SQLAlchemy

Install [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy) and [psycopg3](https://www.psycopg.org/psycopg3/docs/basic/install.html)
```bash
pip install "psycopg[binary]" sqlalchemy
```

Then write your code. See [examples/sqlalchemy_example.py](examples/sqlalchemy_example.py) and [tests/test_sqlalchemy.py](tests/test_sqlalchemy.py) for example.

All the operators include:
- `squared_euclidean_distance`
- `negative_dot_product_distance`
- `negative_cosine_distance`

### psycopg3

Install [psycopg3](https://www.psycopg.org/psycopg3/docs/basic/install.html)
```bash
pip install "psycopg[binary]"
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