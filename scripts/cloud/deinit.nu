# Stop the Tangram servers before removing their stores and state.
if not ('.tangram/cloud' | path exists --no-symlink) {
	print -e 'there is no local cloud state to remove'
	exit 0
}
let target = cargo metadata --format-version 1 --no-deps | from json | get target_directory
let scylla = $target | path join release tangram_scylla_client

# Clear FoundationDB with the cloud:databases container's client on macOS, because the host client may not match its version.
let cluster = open --raw .tangram/cloud/fdb.cluster | str trim
let command = 'writemode on; clearrange "tangram_cloud/" "tangram_cloud0"'
if $nu.os-info.name == 'macos' and $cluster == 'docker:docker@127.0.0.1:4500' and (docker ps --quiet --filter 'name=^tangram_cloud_foundationdb$' | is-not-empty) {
	docker exec tangram_cloud_foundationdb fdbcli --timeout 10 --exec $command
} else {
	fdbcli -C .tangram/cloud/fdb.cluster --timeout 10 --exec $command
}
dropdb -U postgres -h 127.0.0.1 --if-exists --force tangram_cloud
^$scylla -e 'drop keyspace if exists tangram_cloud;'

# Restore owner access to read-only artifact directories before removing them.
^find .tangram/cloud -type d -exec chmod u+rwx '{}' ';'
rm -rf .tangram/cloud
