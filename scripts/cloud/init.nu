# Initialize the local cloud stores. Run once while the databases are running.
def main [--fdb-cluster: path] {
	if ('.tangram/cloud' | path exists --no-symlink) {
		error make { msg: 'local cloud state already exists; run cloud:deinit before initializing again' }
	}
	let cluster = if $fdb_cluster != null {
		open --raw $fdb_cluster
	} else if $nu.os-info.name == 'linux' {
		'local:local@127.0.0.1:4500'
	} else {
		'docker:docker@127.0.0.1:4500'
	}

	cargo build --all-features --package tangram_cli --package tangram_scylla_client
	let target = cargo metadata --format-version 1 --no-deps | from json | get target_directory
	let scylla = $target | path join debug tangram_scylla_client
	let tangram = $target | path join debug tangram

	let directory = '.tangram/cloud'
	mkdir $directory
	$cluster | save -f ($directory | path join fdb.cluster)

	createdb -U postgres -h 127.0.0.1 tangram_cloud
	psql -U postgres -h 127.0.0.1 -d tangram_cloud -v ON_ERROR_STOP=1 -f packages/server/src/database/postgres.sql
	^$scylla -e "create keyspace tangram_cloud with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };"
	^$scylla -k tangram_cloud -f packages/cache/scylla/schema.cql

	bytes build 0x[00 00 10 00] (random binary 16) | ^$tangram id | str trim | save -f ($directory | path join indexer)
}
