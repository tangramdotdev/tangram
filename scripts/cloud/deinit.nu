cockroach sql --insecure --host=localhost:26257 -e 'drop database if exists database' | ignore

let cluster_path = mktemp -t
"docker:docker@localhost:4500" | save -f $cluster_path
fdbcli -C $cluster_path --exec 'writemode on; clearrange "" \xff' | ignore

nats stream rm -f processes_finalize_queue | ignore
nats stream rm -f processes_signals | ignore
nats stream rm -f processes_stdio | ignore

dropdb -U postgres -h localhost register | ignore

cqlsh -e 'drop keyspace store;' | ignore
