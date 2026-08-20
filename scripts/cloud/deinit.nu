dropdb -U postgres -h localhost --if-exists --force database | ignore

let cluster_path = mktemp -t
"docker:docker@localhost:4500" | save -f $cluster_path
fdbcli -C $cluster_path --exec 'writemode on; clearrange "" \xff' | ignore

dropdb -U postgres -h localhost --if-exists --force processes | ignore

cqlsh -e 'drop keyspace objects;' | ignore
