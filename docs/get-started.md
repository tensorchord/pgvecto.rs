# Get started

Run the following SQL to ensure the extension is enabled.

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
```

pgvecto.rs introduces a new data type `vector(n)` denoting an n-dimensional vector. The `n` within the brackets signifies the dimensions of the vector.

You could create a table with the following SQL. 

```sql
-- create table with a vector column

CREATE TABLE items (
  id bigserial PRIMARY KEY,
  embedding vector(3) NOT NULL -- 3 dimensions
);
```

You can then populate the table with vector data as follows.

```sql
-- insert values

INSERT INTO items (embedding)
VALUES ('[1,2,3]'), ('[4,5,6]');

-- or insert values using a casting from array to vector

INSERT INTO items (embedding)
VALUES (ARRAY[1, 2, 3]::real[]), (ARRAY[4, 5, 6]::real[]);
```

We support three operators to calculate the distance between two vectors.

- `<->`: squared Euclidean distance, defined as $\Sigma (x_i - y_i) ^ 2$.
- `<#>`: negative dot product, defined as $- \Sigma x_iy_i$.
- `<=>`: negative cosine similarity, defined as $- \frac{\Sigma x_iy_i}{\sqrt{\Sigma x_i^2 \Sigma y_i^2}}$.

```sql
-- call the distance function through operators

-- squared Euclidean distance
SELECT '[1, 2, 3]'::vector <-> '[3, 2, 1]'::vector;
-- negative dot product
SELECT '[1, 2, 3]'::vector <#> '[3, 2, 1]'::vector;
-- negative cosine similarity
SELECT '[1, 2, 3]'::vector <=> '[3, 2, 1]'::vector;
```

You can search for a vector simply like this.

```sql
-- query the similar embeddings
SELECT * FROM items ORDER BY embedding <-> '[3,2,1]' LIMIT 5;
```

## Half-precision floating-point

`vecf16` type is the same with `vector` in anything but the scalar type. It stores 16-bit floating point numbers. If you want to reduce the memory usage to get better performance, you can try to replace `vector` type with `vecf16` type.

## Things You Need to Know

`vector(n)` is a valid data type only if $1 \leq n \leq 65535$. Due to limits of PostgreSQL, it's possible to create a value of type `vector(3)` of $5$ dimensions and `vector` is also a valid data. However, you cannot still put $0$ scalar or more than $65535$ scalars to a vector. If you use `vector` for a column or there is some values mismatched with dimension denoted by the column, you won't able to create an index on it.
