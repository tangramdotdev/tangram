use ../../util/test.nu *

# A watch that is used regularly with no changes remains past its ttl since it was created.

let server = server spawn --now 2026-01-01T00:00:00Z --config {
	advanced: { checkpoints: true }
	watch: { ttl: { secs: 7, nanos: 0 } }
}
let path = artifact { file: 'hello' }

# Check in with a watch, and wait for its ttl timer to start.
let time_to_live = checkpoint watch clock.sleep --params { duration: '7s' }
let id = tg checkin --no-tokens --watch $path | referent node
checkpoint pass $time_to_live | ignore

# Check in the unchanged directory again before the ttl elapses, and wait for its ttl timer to start again.
advance_time $server 4sec
let output = tg checkin --no-tokens --watch $path | referent node
assert equal $output $id
checkpoint pass $time_to_live 1 | ignore
checkpoint unwatch $time_to_live

# Advance the clock past the ttl since the watch was created.
advance_time $server 4sec

# The watch remains.
assert equal (tg watch list | from json | get path) [$path] 'the watch should remain while it is used'
