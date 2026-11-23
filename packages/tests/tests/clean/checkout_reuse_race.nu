use ../../util/test.nu *

# A checkout that is reused after the cleaner decides to remove it and before the cleaner removes its files survives the removal.

let server = server spawn --now 2026-01-01T00:00:00Z --config {
	advanced: { checkpoints: true }
	checkout: { ttt: 1 }
	indexer: { cleaning: {} }
	object: { ttl: 10, ttt: 0 }
	vfs: false
}
let id = tg put --no-tokens 'tg.directory({ "hello.txt": "hello" })' | referent node
let path = tg checkout $id | str trim
tg index

# Touch the object, so that only the checkout expires when the clock passes its ttl.
advance_time $server 6sec
tg touch $id
tg index

# Hold the cleaner after it decides to remove the checkout.
let delete = checkpoint watch cleaning.checkout.delete --params { checkout: $id }
let deleted = checkpoint watch cleaning.checkout.deleted --params { checkout: $id }
advance_time $server 5sec
checkpoint wait $delete | ignore

# Reuse the checkout while the cleaner is held.
let reused = tg checkout $id | str trim
assert equal $reused $path

# Let the cleaner continue and wait for it to finish removing checkouts.
checkpoint continue $delete
checkpoint pass $deleted | ignore
checkpoint unwatch $delete
checkpoint unwatch $deleted

# The reused checkout is still usable.
assert equal (open ($reused | path join hello.txt)) 'hello'
