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

### SQLAlchemy

Install [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy) and [psycopg3](https://www.psycopg.org/psycopg3/docs/basic/install.html)
```bash
pip install "psycopg[binary]" sqlalchemy
```

Then write your code. For example:
```python
import numpy as np
from sqlalchemy import create_engine, select, insert, types
from sqlalchemy import Integer, String
from pgvector_rs.sqlalchemy import Vector
from sqlalchemy.orm import Session, DeclarativeBase, mapped_column, Mapped

URL = "postgresql+psycopg://<...>"

# Define the ORM model


class Base(DeclarativeBase):
    pass


class Document(Base):
    __tablename__ = "documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    text: Mapped[str] = mapped_column(String)
    embedding: Mapped[np.ndarray] = mapped_column(Vector(3))

    def __repr__(self) -> str:
        return f"{self.text}: {self.embedding}"


# Connect to the DB and create the table
engine = create_engine(URL)
Document.metadata.create_all(engine)

with Session(engine) as session:
    # Insert 3 rows into the table
    t1 = insert(Document).values(text="hello world", embedding=[1, 2, 3])
    t2 = insert(Document).values(text="hello postgres", embedding=[1, 2, 4])
    t3 = insert(Document).values(text="hello pgvecto.rs", embedding=[1, 3, 4])
    for t in [t1, t2, t3]:
        session.execute(t)
    session.commit()

    # Select the row "hello pgvecto.rs"
    stmt = select(Document).where(Document.text == "hello pgvecto.rs")
    target = session.scalar(stmt)

    # Select all the rows and sort them
    # by the squared_euclidean_distance to "hello pgvecto.rs"
    stmt = select(
        Document.text,
        Document.embedding.squared_euclidean_distance(target.embedding).label(
            "distance"
        ),
    ).order_by("distance")
    for doc in session.execute(stmt):
        print(doc)

# Drop the table
Document.metadata.drop_all(engine)
```
The output will be:
```
('hello pgvecto.rs', 0.0)
('hello postgres', 1.0)
('hello world', 2.0)
```

All the operators include:
- `squared_euclidean_distance`
- `negative_dot_product_distance`
- `negative_cosine_distance`

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