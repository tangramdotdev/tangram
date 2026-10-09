# Stop the Tangram servers before removing their stores and state.
let target = cargo metadata --format-version 1 --no-deps | from json | get target_directory
let scylla = $target | path join debug tangram_scylla_client

# Use the matching container client only when the test container is the configured cluster.
let cluster = open --raw .tangram/cloud/fdb.cluster | str trim
let command = 'writemode on; clearrange "tangram_cloud/" "tangram_cloud0"'
if $nu.os-info.name == 'macos' and $cluster == 'docker:docker@127.0.0.1:4500' and (docker ps --quiet --filter 'name=^tangram_test_foundationdb$' | is-not-empty) {
	docker exec tangram_test_foundationdb fdbcli --timeout 10 --exec $command
} else {
	fdbcli -C .tangram/cloud/fdb.cluster --timeout 10 --exec $command
}
dropdb -U postgres -h 127.0.0.1 --if-exists --force tangram_cloud
^$scylla -e 'drop keyspace if exists tangram_cloud;'
let directory = '.tangram/cloud' | path expand
rm -rf $directory
rm .tangram/cloud
