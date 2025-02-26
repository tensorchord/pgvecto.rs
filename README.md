<div align="center">
<h1>pgvecto.rs</h1>
</div>

<p align=center>
<a href="https://discord.gg/KqswhpVgdU"><img alt="discord invitation link" src="https://img.shields.io/discord/974584200327991326?style=flat&logo=discord&cacheSeconds=60"></a>
<a href="https://twitter.com/TensorChord"><img src="https://img.shields.io/twitter/follow/tensorchord?style=flat&logo=X&cacheSeconds=60" alt="trackgit-views" /></a>
<a href="https://hub.docker.com/r/tensorchord/pgvecto-rs"><img src="https://img.shields.io/docker/pulls/tensorchord/pgvecto-rs" /></a>
<a href="https://github.com/tensorchord/pgvecto.rs#contributors-"><img alt="all-contributors" src="https://img.shields.io/github/all-contributors/tensorchord/pgvecto.rs/main"></a>
</p>

`pgvecto.rs` is a Postgres extension that provides vector similarity search functions. It is written in Rust and based on [pgrx](https://github.com/pgcentralfoundation/pgrx).

> [!NOTE]
> We have a new implementation [VectorChord](https://github.com/tensorchord/VectorChord/) with better stability and performance. Users are encouraged to migrate to VectorChord. Check out the [migration guide](https://docs.vectorchord.ai/vectorchord/admin/migration.html).

## Comparison with pgvector

Checkout [pgvecto.rs vs pgvector](https://docs.vectorchord.ai/faqs/comparison-pgvector.html) for more details.

| Feature | pgvecto.rs | pgvector |
| --- | --- | --- |
| Filtering | Introduces VBASE method for vector search and relational query (e.g. Single-Vector TopK + Filter + Join). | When filters are applied, the results may be incomplete. For example, if you originally intended to limit the results to 10, you might end up with only 5 results with filters. |
| Vector Dimensions | Supports up to 65535 dimensions. | Supports up to 2000 dimensions. |
| SIMD | SIMD instructions are dynamically dispatched at runtime to maximize performance based on the capabilities of the specific machine. | Added CPU dispatching for distance functions on Linux x86-64" in 0.7.0. |
| Data Types | Introduces additional data types: binary vectors, FP16 (16-bit floating point), and INT8 (8-bit integer). | \- |
| Indexing | Handles the storage and memory of indexes separately from PostgreSQL | Relies on the native storage engine of PostgreSQL |
| WAL Support | Provides Write-Ahead Logging (WAL) support for data, index support is working in progress. | Provides Write-Ahead Logging (WAL) support for index and data. |                         |

## [Documentation](https://docs.vectorchord.ai/getting-started/overview.html)

- Getting Started
  - [Overview](https://docs.vectorchord.ai/getting-started/overview.html)
  - [Installation](https://docs.vectorchord.ai/getting-started/installation.html)
- Usage
  - [Indexing](https://docs.vectorchord.ai/usage/indexing.html)
  - [Search](https://docs.vectorchord.ai/usage/search.html)
- Administration
  - [Configuration](https://docs.vectorchord.ai/admin/configuration.html)
  - [Upgrading from older versions](https://docs.vectorchord.ai/admin/upgrading.html)
- Developers
  - [Development Tutorial](https://docs.vectorchord.ai/developers/development.html)

## Quick start

For new users, we recommend using the [Docker image](https://github.com/tensorchord/pgvecto.rs/pkgs/container/pgvecto-rs) to get started quickly.

```sh
docker run \
  --name pgvecto-rs-demo \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -p 5432:5432 \
  -d ghcr.io/tensorchord/pgvecto-rs:pg17-v0.4.0
```

Then you can connect to the database using the `psql` command line tool. The default username is `postgres`, and the default password is `mysecretpassword`.

```sh
psql -h localhost -p 5432 -U postgres
```

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

> [!TIP]
>`vector(n)` is a valid data type only if $1 \leq n \leq 65535$. Due to limits of PostgreSQL, it's possible to create a value of type `vector(3)` of $5$ dimensions and `vector` is also a valid data type. However, you cannot still put $0$ scalar or more than $65535$ scalars to a vector. If you use `vector` for a column or there is some values mismatched with dimension denoted by the column, you won't able to create an index on it.

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
- `<=>`: cosine distance, defined as $1 - \frac{\Sigma x_iy_i}{\sqrt{\Sigma x_i^2 \Sigma y_i^2}}$.

```sql
-- call the distance function through operators

-- squared Euclidean distance
SELECT '[1, 2, 3]'::vector <-> '[3, 2, 1]'::vector;
-- negative dot product
SELECT '[1, 2, 3]'::vector <#> '[3, 2, 1]'::vector;
-- cosine distance
SELECT '[1, 2, 3]'::vector <=> '[3, 2, 1]'::vector;
```

You can search for a vector simply like this.

```sql
-- query the similar embeddings
SELECT * FROM items ORDER BY embedding <-> '[3,2,1]' LIMIT 5;
```

### A simple Question-Answering application

Please check out the [Question-Answering application](https://docs.vectorchord.ai/use-case/question-answering.html) tutorial.

### Half-precision floating-point

`vecf16` type is the same with `vector` in anything but the scalar type. It stores 16-bit floating point numbers. If you want to reduce the memory usage to get better performance, you can try to replace `vector` type with `vecf16` type.

## Roadmap 🗂️

Please check out [ROADMAP](https://docs.vectorchord.ai/community/roadmap.html). Want to jump in? Welcome discussions and contributions!

- Chat with us on [💬 Discord](https://discord.gg/KqswhpVgdU)
- Have a look at [`good first issue 💖`](https://github.com/tensorchord/pgvecto.rs/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue+%E2%9D%A4%EF%B8%8F%22) issues!

## Contribute 😊

We welcome all kinds of contributions from the open-source community, individuals, and partners.

- Join our [discord community](https://discord.gg/KqswhpVgdU)!
- To build from the source, please read our [contributing documentation](https://docs.vectorchord.ai/community/contributing.html) and [development tutorial](https://docs.vectorchord.ai/developers/development.html).

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
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/JoePassanante"><img src="https://avatars.githubusercontent.com/u/28711605?v=4?s=70" width="70px;" alt="Joe Passanante"/><br /><sub><b>Joe Passanante</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=JoePassanante" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://blog.mapotofu.org/"><img src="https://avatars.githubusercontent.com/u/12974685?v=4?s=70" width="70px;" alt="Keming"/><br /><sub><b>Keming</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/issues?q=author%3Akemingy" title="Bug reports">🐛</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=kemingy" title="Code">💻</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=kemingy" title="Documentation">📖</a> <a href="#ideas-kemingy" title="Ideas, Planning, & Feedback">🤔</a> <a href="#infra-kemingy" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://blog.ymzymz.me"><img src="https://avatars.githubusercontent.com/u/78400701?v=4?s=70" width="70px;" alt="Mingzhuo Yin"/><br /><sub><b>Mingzhuo Yin</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=silver-ymz" title="Code">💻</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=silver-ymz" title="Tests">⚠️</a> <a href="#infra-silver-ymz" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://usamoi.com"><img src="https://avatars.githubusercontent.com/u/79277854?v=4?s=70" width="70px;" alt="Usamoi"/><br /><sub><b>Usamoi</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=usamoi" title="Code">💻</a> <a href="#ideas-usamoi" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/cutecutecat"><img src="https://avatars.githubusercontent.com/u/19801166?v=4?s=70" width="70px;" alt="cutecutecat"/><br /><sub><b>cutecutecat</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=cutecutecat" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/odysa"><img src="https://avatars.githubusercontent.com/u/22908409?v=4?s=70" width="70px;" alt="odysa"/><br /><sub><b>odysa</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=odysa" title="Documentation">📖</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=odysa" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/my-vegetable-has-exploded"><img src="https://avatars.githubusercontent.com/u/48236141?v=4?s=70" width="70px;" alt="yi wang"/><br /><sub><b>yi wang</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=my-vegetable-has-exploded" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://yihong.run"><img src="https://avatars.githubusercontent.com/u/15976103?v=4?s=70" width="70px;" alt="yihong"/><br /><sub><b>yihong</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=yihong0618" title="Code">💻</a></td>
    </tr>
    <tr>
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
