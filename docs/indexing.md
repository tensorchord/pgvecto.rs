# Indexing

Indexing is the core ability of pgvecto.rs.

Assuming there is a table `items` and there is a column named `embedding` of type `vector(n)`, you can create a vector index for squared Euclidean distance with the following SQL.

```sql
CREATE INDEX ON items USING vectors (embedding l2_ops);
```

For negative dot product, replace `l2_ops` with `dot_ops`.
For negative cosine similarity, replace `l2_ops` with `cosine_ops`.

Now you can perform a KNN search with the following SQL again, but this time the vector index is used for searching.

```sql
SELECT * FROM items ORDER BY embedding <-> '[3,2,1]' LIMIT 5;
```

## Things You Need to Know

pgvecto.rs constructs the index asynchronously. When you insert new rows into the table, they will first be placed in an append-only file. The background thread will periodically merge the newly inserted row to the existing index. When a user performs any search prior to the merge process, it scans the append-only file to ensure accuracy and consistency.

## Options

We utilize TOML syntax to express the index's configuration. Here's what each key in the configuration signifies:

| Key        | Type  | Description                            |
| ---------- | ----- | -------------------------------------- |
| segment    | table | Options for segments.                  |
| optimizing | table | Options for background optimizing.     |
| indexing   | table | The algorithm to be used for indexing. |

Options for table `segment`.

| Key                      | Type    | Description                                                         |
| ------------------------ | ------- | ------------------------------------------------------------------- |
| max_growing_segment_size | integer | Maximum size of unindexed vectors. Default value is `20_000`.       |
| min_sealed_segment_size  | integer | Minimum size of vectors for indexing. Default value is `1_000`.     |
| max_sealed_segment_size  | integer | Maximum size of vectors for indexing. Default value is `1_000_000`. |

Options for table `optimizing`.

| Key                | Type    | Description                                                                 |
| ------------------ | ------- | --------------------------------------------------------------------------- |
| optimizing_threads | integer | Maximum threads for indexing. Default value is the sqrt of number of cores. |

Options for table `indexing`.

| Key  | Type  | Description                                                             |
| ---- | ----- | ----------------------------------------------------------------------- |
| flat | table | If this table is set, brute force algorithm will be used for the index. |
| ivf  | table | If this table is set, IVF will be used for the index.                   |
| hnsw | table | If this table is set, HNSW will be used for the index.                  |

You can choose only one algorithm in above indexing algorithms. Default value is `hnsw`.

Options for table `flat`.

| Key          | Type  | Description                                |
| ------------ | ----- | ------------------------------------------ |
| quantization | table | The algorithm to be used for quantization. |

Options for table `ivf`.

| Key              | Type    | Description                                                     |
| ---------------- | ------- | --------------------------------------------------------------- |
| nlist            | integer | Number of cluster units. Default value is `1000`.               |
| nprobe           | integer | Number of units to query. Default value is `10`.                |
| least_iterations | integer | Least iterations for K-Means clustering. Default value is `16`. |
| iterations       | integer | Max iterations for K-Means clustering. Default value is `500`.  |
| quantization     | table   | The quantization algorithm to be used.                          |

Options for table `hnsw`.

| Key             | Type    | Description                                        |
| --------------- | ------- | -------------------------------------------------- |
| m               | integer | Maximum degree of the node. Default value is `12`. |
| ef_construction | integer | Search scope in building. Default value is `300`.  |
| quantization    | table   | The quantization algorithm to be used.             |

Options for table `quantization`.

| Key     | Type  | Description                                         |
| ------- | ----- | --------------------------------------------------- |
| trivial | table | If this table is set, no quantization is used.      |
| scalar  | table | If this table is set, scalar quantization is used.  |
| product | table | If this table is set, product quantization is used. |

You can choose only one algorithm in above indexing algorithms. Default value is `trivial`.

Options for table `product`.

| Key    | Type    | Description                                                                                                              |
| ------ | ------- | ------------------------------------------------------------------------------------------------------------------------ |
| sample | integer | Samples to be used for quantization. Default value is `65535`.                                                           |
| ratio  | string  | Compression ratio for quantization. Only `"x4"`, `"x8"`, `"x16"`, `"x32"`, `"x64"` are allowed. Default value is `"x4"`. |

## Progress View

We also provide a view `pg_vector_index_info` to monitor the progress of indexing.
Note that whether idx_sealed_len is equal to idx_tuples doesn't relate to the completion of indexing.
It may do further optimization after indexing. It may also stop indexing because there are too few tuples left.

| Column          | Type   | Description                                   |
| --------------- | ------ | --------------------------------------------- |
| tablerelid      | oid    | The oid of the table.                         |
| indexrelid      | oid    | The oid of the index.                         |
| tablename       | name   | The name of the table.                        |
| indexname       | name   | The name of the index.                        |
| indexing        | bool   | Whether the background thread is indexing.    |
| idx_tuples      | int4   | The number of tuples.                         |
| idx_sealed_len  | int4   | The number of tuples in sealed segments.      |
| idx_growing_len | int4   | The number of tuples in growing segments.     |
| idx_write       | int4   | The number of tuples in write buffer.         |
| idx_sealed      | int4[] | The number of tuples in each sealed segment.  |
| idx_growing     | int4[] | The number of tuples in each growing segment. |
| idx_config      | text   | The configuration of the index.               |

## Examples

There are some examples.

```sql
-- HNSW algorithm, default settings.

CREATE INDEX ON items USING vectors (embedding l2_ops);

--- Or using bruteforce with PQ.

CREATE INDEX ON items USING vectors (embedding l2_ops)
WITH (options = $$
[indexing.flat]
quantization.product.ratio = "x16"
$$);

--- Or using IVFPQ algorithm.

CREATE INDEX ON items USING vectors (embedding l2_ops)
WITH (options = $$
[indexing.ivf]
quantization.product.ratio = "x16"
$$);

-- Use more threads for background building the index.

CREATE INDEX ON items USING vectors (embedding l2_ops)
WITH (options = $$
optimizing.optimizing_threads = 16
$$);

-- Prefer smaller HNSW graph.

CREATE INDEX ON items USING vectors (embedding l2_ops)
WITH (options = $$
segment.max_growing_segment_size = 200000
$$);
```
