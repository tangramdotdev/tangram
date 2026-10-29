use ../../util/test.nu *

# A watch registered during checkin is automatically removed after the configured watch TTL elapses.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true },
	watch: {
		ttl: { secs: 1, nanos: 0 }
	}
}

let path = artifact {
	tangram.ts: ''
}

# Check in with a watch, and wait for its TTL timer to start.
let ttl = checkpoint watch clock.sleep --params { duration: '1s' }
let id = tg checkin --no-tokens $path --watch | referent node
checkpoint pass $ttl | ignore
checkpoint unwatch $ttl
let object = tg get $id --blobs --depth=inf --no-tokens --pretty
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob(""),
	    "module": "typescript",
	  }),
	})
'

let watches = tg watch list | from json
assert equal ($watches | length) 1

# The watch remains until its TTL elapses.
advance_time $local 999ms
assert equal (tg watch list | from json | length) 1 "the watch should remain before its ttl expires"

# The watch is removed once its TTL elapses.
let removed = checkpoint watch watch.removed
advance_time $local 1ms
checkpoint pass $removed | ignore
checkpoint unwatch $removed
assert (tg watch list | from json | is-empty) "the watch should be removed after its ttl expires"
