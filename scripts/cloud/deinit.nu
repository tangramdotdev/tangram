dropdb -U postgres -h localhost database | ignore

let cluster_path = mktemp -t
"docker:docker@localhost:4500" | save -f $cluster_path
fdbcli -C $cluster_path --exec 'writemode on; clearrange "" \xff' | ignore

nats stream rm -f finish | ignore
nats stream rm -f queue | ignore

cqlsh -e 'drop keyspace store;' | ignore
