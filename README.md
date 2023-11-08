<div align="center">
<h1>pgvecto.rs</h1>
</div>

<p align=center>
<a href="https://discord.gg/KqswhpVgdU"><img alt="discord invitation link" src="https://dcbadge.vercel.app/api/server/KqswhpVgdU?style=flat"></a>
<a href="https://twitter.com/TensorChord"><img src="https://img.shields.io/twitter/follow/tensorchord?style=social" alt="trackgit-views" /></a>
<a href="https://github.com/tensorchord/pgvecto.rs#contributors-"><img alt="all-contributors" src="https://img.shields.io/github/all-contributors/tensorchord/pgvecto.rs/main"></a>
</p>

pgvecto.rs is a Postgres extension that provides vector similarity search functions. It is written in Rust and based on [pgrx](https://github.com/tcdi/pgrx). It is currently in the beta status, we invite you to try it out in production and provide us with feedback. Read more at [📝our launch blog](https://modelz.ai/blog/pgvecto-rs).

## Why use pgvecto.rs

- 💃 **Easy to use**: pgvecto.rs is a Postgres extension, which means that you can use it directly within your existing database. This makes it easy to integrate into your existing workflows and applications.
- 🥅 **Filtering**: pgvecto.rs supports filtering. You can set conditions when searching or retrieving points. This is the missing feature of other postgres extensions.
- 🚀 **High Performance**: pgvecto.rs is designed to provide significant improvements compared to existing Postgres extensions. Benchmarks have shown that its HNSW index can deliver search performance up to 20 times faster than other indexes like ivfflat.
- 🔧 **Extensible**: pgvecto.rs is designed to be extensible. It is easy to add new index structures and search algorithms. This flexibility ensures that pgvecto.rs can adapt to emerging vector search algorithms and meet diverse performance needs.
- 🦀 **Rewrite in Rust**: Rust's strict compile-time checks ensure memory safety, reducing the risk of bugs and security issues commonly associated with C extensions.
- 🙋 **Community Driven**: We encourage community involvement and contributions, fostering innovation and continuous improvement.

## Comparison with pgvector

|                                             | pgvecto.rs                        | pgvector                  |
| ------------------------------------------- | --------------------------------- | ------------------------- |
| Transaction support                         | ✅                                 | ⚠️                         |
| Sufficient Result with Delete/Update/Filter | ✅                                 | ⚠️                         |
| Vector Dimension Limit                      | 65535                             | 2000                      |
| Prefilter on HNSW                           | ✅                                 | ❌                         |
| Parallel Index build                        | ⚡️ Linearly faster with more cores | 🐌 Only single core used   |
| Index Persistence                           | mmap file                         | Postgres internal storage |
| WAL amplification                           | 2x 😃                              | 30x 🧐                     |

And based on our benchmark, pgvecto.rs can be up to 2x faster than pgvector on hnsw indexes with same configurations. Read more about the comparison at [here](./docs/comparison-pgvector.md).

## Installation

We recommend you to try pgvecto.rs using our pre-built docker, by running

```bash
docker run --name pgvecto-rs-demo -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d tensorchord/pgvecto-rs:latest
```

For more installation method (binary install or install from source), read more at [docs/install.md](./docs/install.md)

## Get started

Run the following SQL to ensure the extension is enabled

```sql
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
```

pgvecto.rs allows columns of a table to be defined as vectors.

The data type `vector(n)` denotes an n-dimensional vector. The `n` within the brackets signifies the dimensions of the vector. For instance, `vector(1000)` would represent a vector with 1000 dimensions, so you could create a table like this.

```sql
-- create table with a vector column

CREATE TABLE items (
  id bigserial PRIMARY KEY,
  embedding vector(3) NOT NULL
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
- `<#>`: negative dot product distance, defined as $- \Sigma x_iy_i$.
- `<=>`: negative cosine distance, defined as $- \frac{\Sigma x_iy_i}{\sqrt{\Sigma x_i^2 \Sigma y_i^2}}$.

```sql
-- call the distance function through operators

-- squared Euclidean distance
SELECT '[1, 2, 3]'::vector <-> '[3, 2, 1]'::vector;
-- negative dot product distance
SELECT '[1, 2, 3]' <#> '[3, 2, 1]';
-- negative square cosine distance
SELECT '[1, 2, 3]' <=> '[3, 2, 1]';
```

You can search for a vector simply like this.

```sql
-- query the similar embeddings
SELECT * FROM items ORDER BY embedding <-> '[3,2,1]' LIMIT 5;
-- query the neighbors within a certain distance
SELECT * FROM items WHERE embedding <-> '[3,2,1]' < 5;
```

### Indexing

You can create an index, using squared Euclidean distance with the following SQL.

```sql
-- Using HNSW algorithm.

CREATE INDEX ON items USING vectors (embedding l2_ops);

--- Or using bruteforce with PQ.

CREATE INDEX ON items USING vectors (embedding l2_ops)
WITH (options = $$
[indexing.flat]
quantization = { product = { ratio = "x16" } }
$$);

--- Or using IVFPQ algorithm.

CREATE INDEX ON items USING vectors (embedding l2_ops)
WITH (options = $$
[indexing.ivf]
quantization = { product = { ratio = "x16" } }
$$);

--- Or using Vamana algorithm.

CREATE INDEX ON items USING vectors (embedding l2_ops)
WITH (options = $$
[indexing.vamana]
$$);
```

Now you can perform a KNN search with the following SQL simply.

```sql
SELECT *, embedding <-> '[0, 0, 0]' AS score
FROM items
ORDER BY embedding <-> '[0, 0, 0]' LIMIT 10;
```

We planning to support more index types ([issue here](https://github.com/tensorchord/pgvecto.rs/issues/17)).

Welcome to contribute if you are also interested!

## Why not a specialized vector database?

Read our blog at [modelz.ai/blog/pgvector](https://modelz.ai/blog/pgvector)

## Reference

### `vector` type

`vector` and `vector(n)` are all legal data types, where `n` denotes dimensions of a vector.

The current implementation ignores dimensions of a vector, i.e., the behavior is the same as for vectors of unspecified dimensions.

There is only one exception: indexes cannot be created on columns without specified dimensions.

### Indexing

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
| ef_construction | integer | Search scope in building.  Default value is `300`. |
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

And you can change the number of expected result (such as `ef_search` in hnsw) by using the following SQL.

```sql
---  (Optional) Expected number of candidates returned by index
SET vectors.k = 32;
--- Or use local to set the value for the current session
SET LOCAL vectors.k = 32;
```

If you want to disable vector indexing or prefilter, we also offer some GUC options:
- `vectors.enable_vector_index`: Enable or disable the vector index. Default value is `on`.
- `vectors.enable_prefilter`: Enable or disable the prefilter. Default value is `on`.

## Limitations

- The filtering process is not yet optimized. To achieve optimal performance, you may need to manually experiment with different strategies. For example, you can try searching without a vector index or implementing post-filtering techniques like the following query: `select * from (select * from items ORDER BY embedding <-> '[3,2,1]' LIMIT 100 ) where category = 1`. This involves using approximate nearest neighbor (ANN) search to obtain enough results and then applying filtering afterwards.

## Setting up the development environment

You could use [envd](https://github.com/tensorchord/envd) to set up the development environment with one command. It will create a docker container and install all the dependencies for you.

```sh
pip install envd
envd up
````

## Contributing

We need your help! Please check out the [issues](https://github.com/tensorchord/pgvecto.rs/issues).

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://skyzh.dev"><img src="https://avatars.githubusercontent.com/u/4198311?v=4?s=70" width="70px;" alt="Alex Chi"/><br /><sub><b>Alex Chi</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=skyzh" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/AuruTus"><img src="https://avatars.githubusercontent.com/u/33182215?v=4?s=70" width="70px;" alt="AuruTus"/><br /><sub><b>AuruTus</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=AuruTus" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/AveryQi115"><img src="https://avatars.githubusercontent.com/u/42568619?v=4?s=70" width="70px;" alt="Avery"/><br /><sub><b>Avery</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=AveryQi115" title="Code">💻</a> <a href="#ideas-AveryQi115" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://yeya24.github.io/"><img src="https://avatars.githubusercontent.com/u/25150124?v=4?s=70" width="70px;" alt="Ben Ye"/><br /><sub><b>Ben Ye</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=yeya24" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/gaocegege"><img src="https://avatars.githubusercontent.com/u/5100735?v=4?s=70" width="70px;" alt="Ce Gao"/><br /><sub><b>Ce Gao</b></sub></a><br /><a href="#business-gaocegege" title="Business development">💼</a> <a href="#content-gaocegege" title="Content">🖋</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=gaocegege" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/VoVAllen"><img src="https://avatars.githubusercontent.com/u/8686776?v=4?s=70" width="70px;" alt="Jinjing Zhou"/><br /><sub><b>Jinjing Zhou</b></sub></a><br /><a href="#design-VoVAllen" title="Design">🎨</a> <a href="#ideas-VoVAllen" title="Ideas, Planning, & Feedback">🤔</a> <a href="#projectManagement-VoVAllen" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://blog.mapotofu.org/"><img src="https://avatars.githubusercontent.com/u/12974685?v=4?s=70" width="70px;" alt="Keming"/><br /><sub><b>Keming</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/issues?q=author%3Akemingy" title="Bug reports">🐛</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=kemingy" title="Code">💻</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=kemingy" title="Documentation">📖</a> <a href="#ideas-kemingy" title="Ideas, Planning, & Feedback">🤔</a> <a href="#infra-kemingy" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://blog.ymzymz.me"><img src="https://avatars.githubusercontent.com/u/78400701?v=4?s=70" width="70px;" alt="Mingzhuo Yin"/><br /><sub><b>Mingzhuo Yin</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=silver-ymz" title="Code">💻</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=silver-ymz" title="Tests">⚠️</a> <a href="#infra-silver-ymz" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://usamoi.com"><img src="https://avatars.githubusercontent.com/u/79277854?v=4?s=70" width="70px;" alt="Usamoi"/><br /><sub><b>Usamoi</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=usamoi" title="Code">💻</a> <a href="#ideas-usamoi" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/odysa"><img src="https://avatars.githubusercontent.com/u/22908409?v=4?s=70" width="70px;" alt="odysa"/><br /><sub><b>odysa</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=odysa" title="Documentation">📖</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=odysa" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://yihong.run"><img src="https://avatars.githubusercontent.com/u/15976103?v=4?s=70" width="70px;" alt="yihong"/><br /><sub><b>yihong</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=yihong0618" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://yanli.one"><img src="https://avatars.githubusercontent.com/u/32453863?v=4?s=70" width="70px;" alt="盐粒 Yanli"/><br /><sub><b>盐粒 Yanli</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=BeautyyuYanli" title="Code">💻</a></td>
    </tr>
  </tbody>
  <tfoot>
    <tr>
      <td align="center" size="13px" colspan="7">
        <img src="https://raw.githubusercontent.com/all-contributors/all-contributors-cli/1b8533af435da9854653492b1327a23a4dbd0a10/assets/logo-small.svg">
          <a href="https://all-contributors.js.org/docs/en/bot/usage">Add your contributions</a>
        </img>
      </td>
    </tr>
  </tfoot>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!

## Acknowledgements

Thanks to the following projects:

- [pgrx](https://github.com/tcdi/pgrx) - Postgres extension framework in Rust
- [pgvector](https://github.com/pgvector/pgvector) - Postgres extension for vector similarity search written in C
