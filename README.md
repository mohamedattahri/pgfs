# pgfs

[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/mohamed.attahri.com/pgfs)

`pgfs` is a Go library that implements [fs.FS](https://pkg.go.dev/io/fs) on
top of Postgres's
[large objects](https://www.postgresql.org/docs/9.4/largeobjects.html) API.

It's tested on Postgres 14+, and allows the storage of files of any
size up to 4Gb.

## Documentation

See [documentation](https://pkg.go.dev/mohamed.attahri.com/pgfs) for more details.

## Installation

```shell
go get mohamed.attahri.com/pgfs
```

## Testing

Tests require Docker engine to be running, and the Docker CLI
to be installed.

```sh
make test
```
