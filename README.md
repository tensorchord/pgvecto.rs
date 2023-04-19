# pgvectors

Postgres vector similarity search extension.

## Development

- [rust](https://www.rust-lang.org/)
- [pgrx](https://github.com/tcdi/pgrx)

## Usage

```sh
cargo pgrx run
```

```sql
-- install the extension
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
-- check the extension related functions
\df+

-- call the distance function through operators

-- square Euclidean distance
SELECT array[1, 2, 3] <-> array[3, 2, 1];
-- dot product distance
SELECT array[1, 2, 3] <#> array[3, 2, 1];
-- cosine distance
SELECT array[1, 2, 3] <=> array[3, 2, 1];
```
