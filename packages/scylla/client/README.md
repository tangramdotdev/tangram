# tangram_scylla_client

`tangram_scylla_client` is the small CQL command-line client used by Tangram's
test harness. It supports the subset of Apache CQLSH needed to manage test
ScyllaDB keyspaces:

```sh
tangram_scylla_client [host] [port] --execute <cql>
tangram_scylla_client [host] [port] --keyspace <keyspace> --file <path>
```

The client executes semicolon-separated statements, accepts `--` and `/* */`
comments, and prints the JSON value from each row returned by a `SELECT JSON`
statement. It does not provide an interactive shell.
