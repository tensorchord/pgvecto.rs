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

There are two ways to solve the problem:

* Set `vectors.k` larger.
* Set `vectors.enable_prefilter` to `on`. The index will check if the returned row can be accepted by PostgreSQL. However, it will make queries slower so the default value for this option is `off`.

## Options

Search options are specified by PostgreSQL GUC. You can use `SET` command to apply these options in session or `SET LOCAL` command to apply these options in transaction.

| Option                      | Type    | Description                                                                                                                                                   |
| --------------------------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| vectors.k                   | integer | Expected number of candidates returned by index. The parameter will influence the recall if you use HNSW or quantization for indexing. Default value is `64`. |
| vectors.enable_prefilter    | boolean | Enable prefiltering or not. Default value is `off`.                                                                                                           |
| vectors.enable_vector_index | boolean | Enable vector indexes or not. This option is for debugging. Default value is `on`.                                                                            |

