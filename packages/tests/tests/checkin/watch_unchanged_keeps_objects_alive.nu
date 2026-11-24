use ../../util/test.nu *

# A watched directory that is checked in regularly with no changes keeps its objects alive past the object ttl.

let server = server spawn --now 2026-01-01T00:00:00Z --config {
	advanced: { checkpoints: true }
	indexer: { cleaning: {} }
	object: { ttl: 10, ttt: 0 }
	vfs: false
	watch: { ttl: { secs: 100, nanos: 0 } }
}
let path = artifact { file: 'hello' }
let id = tg checkin --no-tokens --watch $path | referent node
tg index

# Put an untouched object whose removal shows that the cleaner has processed everything that expired with it.
let sentinel = tg put --no-tokens 'tg.file("sentinel")' | referent node
tg index
let delete = checkpoint watch cleaning.object.delete --params { object: $sentinel }

# Check in the unchanged directory regularly.
for _ in 1..2 {
	advance_time $server 4sec
	let output = tg checkin --no-tokens --watch $path | referent node
	assert equal $output $id
	tg index
}

# Advance the clock past the ttl since the first checkin, and wait for the cleaner to remove the sentinel.
advance_time $server 4sec
checkpoint pass $delete | ignore
checkpoint unwatch $delete

# The artifact is still usable.
assert equal (tg read $'($id)?get=file') 'hello'
