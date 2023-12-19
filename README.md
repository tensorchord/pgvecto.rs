<div align="center">
<h1>pgvecto.rs</h1>
</div>

<p align=center>
<a href="https://discord.gg/KqswhpVgdU"><img alt="discord invitation link" src="https://dcbadge.vercel.app/api/server/KqswhpVgdU?style=flat"></a>
<a href="https://twitter.com/TensorChord"><img src="https://img.shields.io/twitter/follow/tensorchord?style=social" alt="trackgit-views" /></a>
<a href="https://hub.docker.com/r/tensorchord/pgvecto-rs"><img src="https://img.shields.io/docker/pulls/tensorchord/pgvecto-rs" /></a>
<a href="https://github.com/tensorchord/pgvecto.rs#contributors-"><img alt="all-contributors" src="https://img.shields.io/github/all-contributors/tensorchord/pgvecto.rs/main"></a>
</p>

pgvecto.rs is a Postgres extension that provides vector similarity search functions. It is written in Rust and based on [pgrx](https://github.com/tcdi/pgrx). It is currently in the beta status, we invite you to try it out in production and provide us with feedback. Read more at [📝our launch blog](https://modelz.ai/blog/pgvecto-rs).

## Why use pgvecto.rs

- 💃 **Easy to use**: pgvecto.rs is a Postgres extension, which means that you can use it directly within your existing database. This makes it easy to integrate into your existing workflows and applications.
- 🔗 **Async indexing**: pgvecto.rs's index is asynchronously constructed by the background threads and does not block insertions and always ready for new queries.
- 🥅 **Filtering**: pgvecto.rs supports filtering. You can set conditions when searching or retrieving points. This is the missing feature of other postgres extensions.
- 🧮 **Quantization**: pgvecto.rs supports scalar quantization and product qutization up to 64x.
- 🦀 **Rewrite in Rust**: Rust's strict compile-time checks ensure memory safety, reducing the risk of bugs and security issues commonly associated with C extensions.

## Comparison with pgvector

|                                             | pgvecto.rs                                             | pgvector                |
| ------------------------------------------- | ------------------------------------------------------ | ----------------------- |
| Transaction support                         | ✅                                                      | ⚠️                       |
| Sufficient Result with Delete/Update/Filter | ✅                                                      | ⚠️                       |
| Vector Dimension Limit                      | 65535                                                  | 2000                    |
| Prefilter on HNSW                           | ✅                                                      | ❌                       |
| Parallel HNSW Index build                   | ⚡️ Linearly faster with more cores                      | 🐌 Only single core used |
| Async Index build                           | Ready for queries anytime and do not block insertions. | ❌                       |
| Quantization                                | Scalar/Product Quantization                            | ❌                       |

More details at [./docs/comparison-pgvector.md](./docs/comparison-pgvector.md)

## Documentation

- [Installation](./docs/installation.md)
- [Get Started](./docs/get-started.md)
- [Indexing](./docs/indexing.md)
- [Searching](./docs/searching.md)
- [Comparison with pgvector](./docs/comparison-pgvector.md)
- [Why not a specialty vector database?](./docs/comparison-with-specialized-vectordb.md)

For users, we recommend you to try pgvecto.rs using our pre-built docker image, by running

```sh
docker run \
  --name pgvecto-rs-demo \
  -e POSTGRES_PASSWORD=mysecretpassword \
  -p 5432:5432 \
  -d tensorchord/pgvecto-rs:pg16-v0.1.13
```

## Development with envd

For developers, you could use [envd](https://github.com/tensorchord/envd) to set up the development environment with one command. It will create a docker container and install all the dependencies for you.

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
