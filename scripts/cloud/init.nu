# Initialize the local cloud stores. Run once while the databases are running.
def main [--fdb-cluster: path] {
	if ('.tangram/cloud' | path exists --no-symlink) {
		error make { msg: 'local cloud state already exists; run cloud:deinit before initializing again' }
	}
	cargo build --all-features --package tangram_cli --package tangram_scylla_client
	let target = cargo metadata --format-version 1 --no-deps | from json | get target_directory
	let scylla = $target | path join debug tangram_scylla_client
	let tangram = $target | path join debug tangram

	# Keep sandbox Unix socket paths short, regardless of the checkout location.
	let directory = mktemp -d --tmpdir-path /tmp tg-cloud.XXXXXX
	mkdir .tangram
	ln -s $directory .tangram/cloud
	ln -s $tangram ($directory | path join tangram)
	let cluster = if $fdb_cluster != null {
		open --raw $fdb_cluster
	} else if $nu.os-info.name == 'linux' {
		'local:local@127.0.0.1:4500'
	} else {
		'docker:docker@127.0.0.1:4500'
	}
	$cluster | save -f ($directory | path join fdb.cluster)

	createdb -U postgres -h 127.0.0.1 tangram_cloud
	psql -U postgres -h 127.0.0.1 -d tangram_cloud -v ON_ERROR_STOP=1 -f packages/server/src/database/postgres.sql
	^$scylla -e "create keyspace tangram_cloud with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };"
	^$scylla -k tangram_cloud -f packages/cache/scylla/schema.cql

	{
		advanced: { disable_version_check: true },
		remotes: { default: { url: 'http://127.0.0.1:8476' } },
		vfs: false,
	} | to json | save -f ($directory | path join client.json)
	bytes build 0x[00 00 10 00] (random binary 16) | ^$tangram id | str trim | save -f ($directory | path join indexer)
}
