# CLI for `pgvecto.rs`

## Build

```bash
cargo build -p cli
```

## Usage

- create: configure an index to an empty dir
- add: add vectors from a file [HDF5, fvecs] (Note: this doesn't trigger the index build)
- build: build the index
- query: query the index from a file

```bash
./target/debug/cli --help
```
