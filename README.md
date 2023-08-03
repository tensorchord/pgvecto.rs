<div align="center">
<h1>pgvecto.rs</h1>
</div>

<p align=center>
<a href="https://discord.gg/KqswhpVgdU"><img alt="discord invitation link" src="https://dcbadge.vercel.app/api/server/KqswhpVgdU?style=flat"></a>
<a href="https://twitter.com/TensorChord"><img src="https://img.shields.io/twitter/follow/tensorchord?style=social" alt="trackgit-views" /></a>
<a href="https://github.com/tensorchord/pgvecto.rs#contributors-"><img alt="all-contributors" src="https://img.shields.io/github/all-contributors/tensorchord/pgvecto.rs/main"></a>
</p>

pgvecto.rs is a Postgres extension that provides vector similarity search functions. It is written in Rust and based on [pgrx](https://github.com/tcdi/pgrx). It is currently ⚠️**under heavy development**⚠️, please take care when using it in production. Read more at [📝our launch blog](https://modelz.ai/blog/pgvecto-rs).

## Why use pgvecto.rs

- 💃 **Easy to use**: pgvecto.rs is a Postgres extension, which means that you can use it directly within your existing database. This makes it easy to integrate into your existing workflows and applications.
- 🦀 **Rewrite in Rust**: Rewriting in Rust offers benefits such as improved memory safety, better performance, and reduced **maintenance costs** over time.
- 🙋 **Community**: People loves Rust We are happy to help you with any questions you may have. You could join our [Discord](https://discord.gg/KqswhpVgdU) to get in touch with us.

## Installation from Source

<details>
  <summary>Build from Source</summary>

### Install Rust and base dependency
```sh
apt install -y build-essential libpq-dev libssl-dev pkg-config gcc libreadline-dev flex bison libxml2-dev libxslt-dev libxml2-utils xsltproc zlib1g-dev ccache clang
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Install pgrx (tensorchord's fork)
```sh
cargo install cargo-pgrx --git https://github.com/tensorchord/pgrx.git --rev $(cat Cargo.toml | grep "pgrx =" | awk -F'rev = "' '{print $2}' | cut -d'"' -f1)
cargo pgrx init
```

### Build the extension and config postgres
```sh
cargo pgrx install --release
psql -U postgres -c 'ALTER SYSTEM SET shared_preload_libraries = "vectors"'
```
You need restart your PostgreSQL server for the changes to take effect, like `systemctl restart postgresql.service`.
</details>


## Install the extension in postgres

```sql
-- install the extension
DROP EXTENSION IF EXISTS vectors;
CREATE EXTENSION vectors;
-- check the extension related functions
\df+
```

## Get started with pgvecto.rs

We support three operators to calculate the distance between two vectors:

- `<->`: square Euclidean distance
- `<#>`: negative dot product distance
- `<=>`: negative square cosine distance

```sql
-- call the distance function through operators

-- square Euclidean distance
SELECT '[1, 2, 3]' <-> '[3, 2, 1]';
-- dot product distance
SELECT '[1, 2, 3]' <#> '[3, 2, 1]';
-- cosine distance
SELECT '[1, 2, 3]' <=> '[3, 2, 1]';
```

Note that, "square Euclidean distance" is defined as $ \Sigma (x_i - y_i) ^ 2 $, "negative dot product distance" is defined as $ - \Sigma x_iy_i $, and "negative square cosine distance" is defined as $ - \frac{(\Sigma x_iy_i)^2}{\Sigma x_i^2 \Sigma y_i^2} $, so that you can use `ORDER BY` to perform a KNN search directly without a `DESC` keyword.

### Create a table

You could use the `CREATE TABLE` statement to create a table with a vector column.

```sql
-- create table
CREATE TABLE items (id bigserial PRIMARY KEY, emb vector(3));
-- insert values
INSERT INTO items (emb) VALUES ('[1,2,3]'), ('[4,5,6]');
-- query the similar embeddings
SELECT * FROM items ORDER BY emb <-> '[3,2,1]' LIMIT 5;
-- query the neighbors within a certain distance
SELECT * FROM items WHERE emb <-> '[3,2,1]' < 5;
```

### Create an index

You can create an index, using HNSW algorithm and square Euclidean distance with the following SQL.

```sql
CREATE INDEX ON train USING vectors (emb l2_ops)
WITH (options = $$
capacity = 2097152
size_ram = 4294967296
storage_vectors = "ram"
[algorithm.hnsw]
storage = "ram"
m = 32
ef = 256
$$);
```

Or using IVFFlat algorithm.

```sql
CREATE INDEX ON train USING vectors (emb l2_ops)
WITH (options = $$
capacity = 2097152
size_ram = 2147483648
storage_vectors = "ram"
[algorithm.ivf]
storage = "ram"
nlist = 1000
nprobe = 10
$$);
```

The index must be built on a vector column. Failure to match the actual vector dimension with the dimension type modifier may result in an unsuccessful index building.

The operator class determines the type of distance measurement to be used. At present, `l2_ops`, `dot_ops`, and `cosine_ops` are supported.

You can specify the indexing and the vectors to be stored in the disk by setting `storage_vectors = "disk"`, and `storage = "disk"`. On this condition, `size_disk` must be specified.

Now you can perform a KNN search with the following SQL simply.

```SQL
SELECT *, emb <-> '[0, 0, 0, 0]' AS score FROM items ORDER BY embedding <-> '[0, 0, 0, 0]' LIMIT 10;
```

We planning to support more index types ([issue here](https://github.com/tensorchord/pgvecto.rs/issues/17)).

Welcome to contribute if you are also interested!


## Why not a specialty vector database?

Imagine this, your existing data is stored in a Postgres database, and you want to use a vector database to do some vector similarity search. You have to move your data from Postgres to the vector database, and you have to maintain two databases at the same time. This is not a good idea.

Why not just use Postgres to do the vector similarity search? This is the reason why we build pgvecto.rs. The user journey is like this:

```sql
-- Update the embedding column for the documents table
UPDATE documents SET embedding = ai_embedding_vector(content) WHERE length(embedding) = 0;

-- Create an index on the embedding column
CREATE INDEX ON documents USING vectors (embedding l2_ops) WITH (algorithm = "HNSW");

-- Query the similar embeddings
SELECT * FROM documents ORDER BY embedding <-> ai_embedding_vector('hello world') LIMIT 5;
```

From [SingleStore DB Blog](https://www.singlestore.com/blog/why-your-vector-database-should-not-be-a-vector-database/):

> Vectors and vector search are a data type and query processing approach, not a foundation for a new way of processing data. Using a specialty vector database (SVDB) will lead to the usual problems we see (and solve) again and again with our customers who use multiple specialty systems: redundant data, excessive data movement, lack of agreement on data values among distributed components, extra labor expense for specialized skills, extra licensing costs, limited query language power, programmability and extensibility, limited tool integration, and poor data integrity and availability compared with a true DBMS.


## Setting up the development environment

You could use [envd](https://github.com/tensorchord/envd) to set up the development environment with one command. It will create a docker container and install all the dependencies for you.

```sh
pip install envd
envd up
```

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
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/gaocegege"><img src="https://avatars.githubusercontent.com/u/5100735?v=4?s=70" width="70px;" alt="Ce Gao"/><br /><sub><b>Ce Gao</b></sub></a><br /><a href="#business-gaocegege" title="Business development">💼</a> <a href="#content-gaocegege" title="Content">🖋</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=gaocegege" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/VoVAllen"><img src="https://avatars.githubusercontent.com/u/8686776?v=4?s=70" width="70px;" alt="Jinjing Zhou"/><br /><sub><b>Jinjing Zhou</b></sub></a><br /><a href="#design-VoVAllen" title="Design">🎨</a> <a href="#ideas-VoVAllen" title="Ideas, Planning, & Feedback">🤔</a> <a href="#projectManagement-VoVAllen" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://blog.mapotofu.org/"><img src="https://avatars.githubusercontent.com/u/12974685?v=4?s=70" width="70px;" alt="Keming"/><br /><sub><b>Keming</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/issues?q=author%3Akemingy" title="Bug reports">🐛</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=kemingy" title="Code">💻</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=kemingy" title="Documentation">📖</a> <a href="#ideas-kemingy" title="Ideas, Planning, & Feedback">🤔</a> <a href="#infra-kemingy" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://usamoi.com"><img src="https://avatars.githubusercontent.com/u/79277854?v=4?s=70" width="70px;" alt="Usamoi"/><br /><sub><b>Usamoi</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=usamoi" title="Code">💻</a> <a href="#ideas-usamoi" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/odysa"><img src="https://avatars.githubusercontent.com/u/22908409?v=4?s=70" width="70px;" alt="odysa"/><br /><sub><b>odysa</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=odysa" title="Documentation">📖</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=odysa" title="Code">💻</a></td>
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
