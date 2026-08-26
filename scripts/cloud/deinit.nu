do --capture-errors { cargo build --package tangram_scylla_client }
let target_directory = cargo metadata --format-version 1 --no-deps | from json | get target_directory
let scylla_client_path = $target_directory | path join debug tangram_scylla_client

dropdb -U postgres -h localhost --if-exists --force database | ignore

let cluster_path = mktemp -t
"docker:docker@localhost:4500" | save -f $cluster_path
fdbcli -C $cluster_path --exec 'writemode on; clearrange "" \xff' | ignore

dropdb -U postgres -h localhost --if-exists --force processes | ignore

^$scylla_client_path -e 'drop keyspace store;' | ignore
