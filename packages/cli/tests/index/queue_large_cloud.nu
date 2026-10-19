use ../lib/test.nu *

# An indexer recovers a cloud queue whose pending fragments exceed ScyllaDB's memory limit for unpaged queries.

skip_if_no_cloud

# Index into LMDB, because a FoundationDB transaction cannot hold a directory with this many children.
let local = server spawn --cloud --config {
	advanced: { checkpoints: true },
	index: { kind: 'lmdb', map_size: 4_294_967_296 },
}
let watch = tg --url $local.url checkpoint watch index.batch | from json | get watch

# Put directories whose index items together exceed ScyllaDB's 100 MiB hard limit for unpaged queries. Shared children keep each item above one MiB, and a unique entry makes each directory distinct.
let children = 0..<30_000 | each { |i|
	let hash = $i | into string | fill --alignment right --character '0' --width 51
	$'"($i)":"fil_01($hash)0"'
} | str join ','
let ids = 0..<110 | each { |i|
	let hash = 30_000 + $i | into string | fill --alignment right --character '0' --width 51
	let output = $'{"entries":{"unique":"fil_01($hash)0",($children)}}' | tg --url $local.url object put --no-tokens --bytes --kind directory | complete
	success $output 'the directory must be stored'
	$output.stdout | referent node
}
tg --url $local.url checkpoint wait index.batch $watch 0 | ignore

# Crash the server while the queue holds every fragment, then restart it.
let pid = open ($local.directory | path join 'lock') | into int
kill --signal 9 $pid
if $nu.os-info.name == "linux" {
	^tail --pid $pid -f /dev/null
} else {
	while (ps | where pid == $pid | is-not-empty) { sleep 10ms }
}
let server = server start $local

let output = tg --url $local.url index | complete
success $output 'the indexer must recover the queue after the restart'

# Verify that every directory has indexed metadata after recovery.
for id in $ids {
	let metadata = tg --url $local.url object metadata $id | from json
	assert ($metadata.node.size > 1_048_576) 'the large directory must be indexed after recovery'
}
