# pgfs

[![GoDoc reference](https://img.shields.io/badge/godoc-reference-blue.svg)](https://pkg.go.dev/mohamed.attahri.com/pgfs)
![CI](https://github.com/mohamedattahri/pgfs/actions/workflows/ci.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/mohamedattahri/pgfs/badge.svg)](https://coveralls.io/github/mohamedattahri/pgfs)
[![Go Report Card](https://goreportcard.com/badge/mohamed.attahri.com/pgfs)](https://goreportcard.com/report/mohamed.attahri.com/pgfs)

`pgfs` is a Go library that implements [fs.FS](https://pkg.go.dev/io/fs) using
[Large Objects](https://www.postgresql.org/docs/current/largeobjects.html) on
Postgres.

## Documentation

See [documentation](https://pkg.go.dev/mohamed.attahri.com/pgfs) for more
details.

## Installation

```shell
go get mohamed.attahri.com/pgfs
```

## Testing

Tests require Docker engine to be running and the Docker CLI
to be installed in order to launch an ephemeral Postgres
instance.

```sh
make test
```
