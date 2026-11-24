use ../../util/test.nu *

# A watched directory with no changes that is checked in again after the cleaner removes its objects returns an artifact that is usable.

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

# Advance the clock past the object ttl while the watch is alive, and wait for the cleaner to remove the root object.
let delete = checkpoint watch cleaning.object.delete --params { object: $id }
advance_time $server 11sec
checkpoint pass $delete | ignore
checkpoint unwatch $delete

# Check in the unchanged directory again.
let output = tg checkin --no-tokens --watch $path | referent node
assert equal $output $id

# The artifact is usable.
assert equal (tg read $'($output)?get=file') 'hello'
