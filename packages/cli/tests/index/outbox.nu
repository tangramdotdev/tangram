use ../../test.nu *

# Objects put on a server with no indexer are durably enqueued to the Scylla outbox, survive the server being killed, and are indexed by a second server started on the same path with the indexer enabled.

const repository_path = path self '../../../../'

# This test requires Scylla.
if (($env.TANGRAM_TEST_CLOUD? | default '') | str length) == 0 {
	skip_test 'this test requires the cloud backends'
}

# Create a Scylla keyspace for the object store and outbox.
let keyspace = $'objects_((random chars) | str downcase)'
let scylla_cql = $repository_path | path join 'packages' 'stores' 'object' 'src' 'scylla.cql'
cqlsh -e $"create keyspace \"($keyspace)\" with replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };"
cqlsh -k $keyspace -f $scylla_cql

# Create the directory shared by both servers and the Scylla object store config.
let directory = mktemp -d
let store = { addr: 'localhost:9042', connections: 1, keyspace: $keyspace, kind: 'scylla' }

# Spawn the first server with the indexer and the outbox consumer disabled.
let producer = spawn --name producer --directory $directory --config {
	indexer: false,
	object_outbox: false,
	object: { store: $store },
}

# Put an object tree.
let id = tg --url $producer.url put 'tg.directory({ "a.txt": tg.file("aaa"), "b.txt": tg.file("bbb") })' | str trim

# Wait for the producer's detached outbox enqueue to commit, then kill the server. The producer never indexes the objects itself; it only enqueues them.
sleep 5sec
let pid = open ($producer.directory | path join 'lock') | into int
kill --signal 2 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}

# Spawn the second server on the same path with the indexer and the outbox consumer enabled.
let indexer = spawn --name indexer --directory $directory --config {
	indexer: true,
	object_outbox: true,
	object: { store: $store },
}

# Wait for the outbox to drain and the index to propagate, then verify the metadata.
tg --url $indexer.url index
let metadata = tg --url $indexer.url object metadata $id | from json
assert equal $metadata.subtree.count 5 "the object tree should be indexed by the second server"
