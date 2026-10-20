use ../lib/test.nu *

# Cleaning an object that was put twice deletes every copy of it from a cloud server's cache.

skip_if_no_cloud

let local = server spawn --cloud --now '2026-01-01T00:00:00Z' --config {
	indexer: { cleaning: { poll_interval: 0.01 } },
	object: { ttl: 60, ttt: 0 },
}
let keyspace = open $local.config_path | get cache.keyspace

# Count the cache rows.
def rows [keyspace: string] {
	let output = ^tangram_scylla_client 127.0.0.1 9042 -k $keyspace -e 'select json id, put from objects' | complete
	success $output 'the cache must be readable'
	$output.stdout | lines | where { $in starts-with '{' } | length
}

# Put the same object twice.
let id = '{"entries":{}}' | tg object put --no-tokens --bytes --kind directory | str trim
'{"entries":{}}' | tg object put --no-tokens --bytes --kind directory | ignore
tg index
assert equal (rows $keyspace) 2 'each put must store a row'

# Clean the object.
advance_time $local 2min
wait_until { (rows $keyspace) < 2 } 'cleaning must delete the object'
assert equal (rows $keyspace) 0 'cleaning must delete the rows of both puts'
let output = tg object get $id | complete
failure $output 'the cleaned object must not be readable'
