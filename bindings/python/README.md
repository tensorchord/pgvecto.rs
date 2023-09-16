# python bindings for pgvector.rs
Supports [SQLAlchemy](https://github.com/sqlalchemy/sqlalchemy)

See use example below:
- [SQLAlchemy](#SQLAlchemy)

## SQLAlchemy

Create a table with `VECTOR` column:

```python
from pgvector_rs.sqlalchemy import Vector
from sqlalchemy import create_engine, MetaData, Table, Column

# set connection info for the postgres db
engine = create_engine(URL)
metadata = MetaData()
metadata.bind = engine

# create table
tb_embbeding = Table(
        'tb_test_item',
        metadata,
        Column('id', Integer, primary_key=True),
        Column('embedding', Vector(3)) # a 3-dim VECTOR type
    )
tb_embbeding.create(engine)
```

Insert a vector

```python
with engine.connect() as con:
	con.execute(
		tb_embbeding.insert().values({'id': _id_value, 'embedding': [1, 2, 3]})
	)
	con.commit()
```

Get entries within a certain distance

```python
with engine.connect() as con:
	res = con.execute(
		tb_embbeding.select().filter(tb_embbeding.c.embedding.squared_euclidean_distance([3, 1, 2]) < 5)
	)
	# do something with res
	# ...
	con.commit()

```
