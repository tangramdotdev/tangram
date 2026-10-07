# tangram_scylla_client

`tangram_scylla_client` is the small CQL command-line client used by Tangram's
test harness and local cloud tooling. It supports the CQL operations needed to
manage test ScyllaDB keyspaces:

```sh
tangram_scylla_client [host] [port] --execute <cql>
tangram_scylla_client [host] [port] --keyspace <keyspace> --file <path>
```

The client executes semicolon-separated statements, accepts `--` and `/* */`
comments, and prints the JSON value from each row returned by a `SELECT JSON`
statement. It does not provide an interactive shell.

For authenticated clusters, pass `--username` (or `-u`) and set
`SCYLLA_PASSWORD`, or pass `--password` (or `-p`). Credentials are optional,
but a username and password must be supplied together. `--request-timeout`
sets the connection and per-statement timeout in seconds (default: 30).
For example:

```sh
SCYLLA_PASSWORD=secret tangram_scylla_client scylla 9042 --username cassandra \
  --request-timeout 120 --execute 'SELECT JSON release_version FROM system.local;'
```
