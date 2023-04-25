<div align="center">
<p>pgvecto.rs</p>
</div>

<p align=center>
<a href="https://discord.gg/KqswhpVgdU"><img alt="discord invitation link" src="https://dcbadge.vercel.app/api/server/KqswhpVgdU?style=flat"></a>
<a href="https://twitter.com/TensorChord"><img src="https://img.shields.io/twitter/follow/tensorchord?style=social" alt="trackgit-views" /></a>
<a href="https://github.com/tensorchord/pgvecto.rs#contributors-"><img alt="all-contributors" src="https://img.shields.io/github/all-contributors/tensorchord/pgvecto.rs/main"></a>
</p>

pgvecto.rs is a Postgres extension that provides vector similarity search functions. It is written in Rust and based on [pgrx](https://github.com/tcdi/pgrx).

## Features

- [x] cosine_distance: cosine distance
- [x] dot_product_distance: dot product distance
- [x] square_euclidean_distance: square Euclidean distance

## Build from source

```sh
cargo install cargo-pgrx
cargo pgrx init
cargo pgrx run
```

## Getting Started

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

-- create table
CREATE TABLE items (id bigserial PRIMARY KEY, emb numeric[]);
-- insert values
INSERT INTO items (emb) VALUES (ARRAY[1,2,3]), (ARRAY[4,5,6]);
-- query the similar embeddings
SELECT * FROM items ORDER BY emb <-> ARRAY[3,2,1] LIMIT 5;
-- query the neighbors within a certain distance
SELECT * FROM items WHERE emb <-> ARRAY[3,2,1] < 5;
```

## Roadmap

See the [open issues](https://github.com/tensorchord/pgvecto.rs/issues/5)

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/gaocegege"><img src="https://avatars.githubusercontent.com/u/5100735?v=4?s=70" width="70px;" alt="Ce Gao"/><br /><sub><b>Ce Gao</b></sub></a><br /><a href="#business-gaocegege" title="Business development">💼</a> <a href="#content-gaocegege" title="Content">🖋</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=gaocegege" title="Documentation">📖</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://blog.mapotofu.org/"><img src="https://avatars.githubusercontent.com/u/12974685?v=4?s=70" width="70px;" alt="Keming"/><br /><sub><b>Keming</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/issues?q=author%3Akemingy" title="Bug reports">🐛</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=kemingy" title="Code">💻</a> <a href="https://github.com/tensorchord/pgvecto.rs/commits?author=kemingy" title="Documentation">📖</a> <a href="#ideas-kemingy" title="Ideas, Planning, & Feedback">🤔</a> <a href="#infra-kemingy" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/VoVAllen"><img src="https://avatars.githubusercontent.com/u/8686776?v=4?s=70" width="70px;" alt="Jinjing Zhou"/><br /><sub><b>Jinjing Zhou</b></sub></a><br /><a href="#design-VoVAllen" title="Design">🎨</a> <a href="#ideas-VoVAllen" title="Ideas, Planning, & Feedback">🤔</a> <a href="#projectManagement-VoVAllen" title="Project Management">📆</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://skyzh.dev"><img src="https://avatars.githubusercontent.com/u/4198311?v=4?s=70" width="70px;" alt="Alex Chi"/><br /><sub><b>Alex Chi</b></sub></a><br /><a href="https://github.com/tensorchord/pgvecto.rs/commits?author=skyzh" title="Code">💻</a></td>
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
