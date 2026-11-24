use ../../util/test.nu *

# A file that is checked in again after the cleaner decides to remove its checkout and before the cleaner removes the checkout's files remains readable.

let server = server spawn --now 2026-01-01T00:00:00Z --config {
	advanced: { checkpoints: true }
	checkout: { ttt: 1 }
	indexer: { cleaning: {} }
	object: { ttl: 10, ttt: 0 }
	vfs: false
}
let path = artifact 'hello'
let id = tg checkin --no-tokens --no-ignore --no-watch $path | referent node
tg index

# Hold the cleaner after it decides to remove the checkout.
let delete = checkpoint watch cleaning.checkout.delete --params { checkout: $id }
let deleted = checkpoint watch cleaning.checkout.deleted --params { checkout: $id }
advance_time $server 11sec
checkpoint wait $delete | ignore

# Check in the same file again while the cleaner is held.
let output = tg checkin --no-tokens --no-ignore --no-watch $path | referent node
assert equal $output $id
tg index

# Let the cleaner continue and wait for it to finish removing checkouts.
checkpoint continue $delete
checkpoint pass $deleted | ignore
checkpoint unwatch $delete
checkpoint unwatch $deleted

# The file is still readable.
assert equal (tg read $id) 'hello'
