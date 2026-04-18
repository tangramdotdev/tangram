cockroach sql --insecure --host=localhost:26257 -e 'drop database if exists database' | ignore

let cluster_path = mktemp -t
"docker:docker@localhost:4500" | save -f $cluster_path
fdbcli -C $cluster_path --exec 'writemode on; clearrange "" \xff' | ignore

dropdb -U postgres -h localhost process_store | ignore

cqlsh -e 'drop keyspace object_store;' | ignore
