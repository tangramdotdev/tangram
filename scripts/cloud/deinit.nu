# Stop the Tangram servers before removing their stores and state.
let target = cargo metadata --format-version 1 --no-deps | from json | get target_directory
let scylla = $target | path join debug tangram_scylla_client

fdbcli -C .tangram/cloud/fdb.cluster --timeout 10 --exec 'writemode on; clearrange "tangram_cloud/" "tangram_cloud0"'
dropdb -U postgres -h 127.0.0.1 --if-exists --force tangram_cloud
^$scylla -e 'drop keyspace if exists tangram_cloud;'
rm -rf .tangram/cloud
