# Searching

The SQL will fetch the $5$ nearest embedding in table `items`.

```sql
SELECT * FROM items ORDER BY embedding <-> '[3,2,1]' LIMIT 5;
```

## Things You Need to Know

If `vectors.k` is set to `64`, but your SQL returned less than `64` rows, for example, only `32` rows. There is some possible reasons:

* Less than `64` rows should be returned. It's expected.
* The vector index returned `64` rows, but `32` of which are deleted before but the index do not know since PostgreSQL vacuum is lazy.
* The vector index returned `64` rows, but `32` of which are invisble to the transaction so PostgreSQL decided to hide these rows for you.
* The vector index returned `64` rows, but `32` of which are satifying the condition `id % 2 = 0` in `WHERE` clause.

There are four ways to solve the problem:

* Set `vectors.k` larger. If you estimate that 20% of rows will satisfy the condition in `WHERE`, just set `vectors.k` to be 5 times than before.
* Set `vectors.enable_vector_index` to `off`. If you estimate that 0.0001% of rows will satisfy the condition in `WHERE`, just do not use vector index. No alogrithms will be faster than brute force by PostgreSQL.
* Set `vectors.enable_prefilter` to `on`. If you cannot estimate how many rows will satisfy the condition in `WHERE`, leave the job for the index. The index will check if the returned row can be accepted by PostgreSQL. However, it will make queries slower so the default value for this option is `off`.
* Set `vectors.vbase_range` to a non-zero value. It will use vbase optimization, so that the index will pull rows as many as you need. It only works for HNSW algorithm.

## Options

Search options are specified by PostgreSQL GUC. You can use `SET` command to apply these options in session or `SET LOCAL` command to apply these options in transaction.

| Option                      | Type    | Description                                                                                                                                                   |
| --------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| vectors.k                   | integer | Expected number of candidates returned by index. The parameter will influence the recall if you use HNSW or quantization for indexing. Default value is `64`. |
| vectors.enable_prefilter    | boolean | Enable prefiltering or not. Default value is `off`.                                                                                                           |
| vectors.enable_vector_index | boolean | Enable vector indexes or not. This option is for debugging. Default value is `on`.                                                                            |
| vectors.vbase_range         | integer | Range size of vbase optimization. When it is set to `0`, vbase optimization will be disabled. A recommended value is `86`. Default value is `0`.              |

Note: When `vectors.vbase_range` is a non-zero value, prefilter does not work.
