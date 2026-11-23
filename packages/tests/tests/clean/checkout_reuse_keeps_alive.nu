use ../../util/test.nu *

# A checkout that is reused regularly is not removed when its ttl passes since it was created.

let server = server spawn --now 2026-01-01T00:00:00Z --config {
	advanced: { checkpoints: true }
	checkout: { ttt: 1 }
	indexer: { cleaning: {} }
	object: { ttl: 10, ttt: 0 }
	vfs: false
}
let id = tg put --no-tokens 'tg.directory({ "hello.txt": "hello" })' | referent node
let path = tg checkout $id | str trim

# Put an untouched object whose removal shows that the cleaner has processed everything that expired with it.
let sentinel = tg put --no-tokens 'tg.file("sentinel")' | referent node
tg index
let delete = checkpoint watch cleaning.object.delete --params { object: $sentinel }

# Reuse the checkout regularly.
for _ in 1..2 {
	advance_time $server 4sec
	tg touch $id
	let reused = tg checkout $id | str trim
	assert equal $reused $path
	tg index
}

# Advance the clock past the ttl since the checkout was created, and wait for the cleaner to remove the sentinel.
advance_time $server 4sec
checkpoint pass $delete | ignore
checkpoint unwatch $delete

# The reused checkout is still usable.
assert equal (open ($path | path join hello.txt)) 'hello'
