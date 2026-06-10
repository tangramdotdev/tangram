use ../../test.nu *

# A watch registered during checkin is automatically removed after the configured watch TTL elapses.

let server = spawn --config {
	watch: {
		ttl: { secs: 1, nanos: 0 }
	}
}

let path = artifact {
	tangram.ts: ''
}

let id = tg checkin $path --watch
let object = tg get $id --blobs --depth=inf --pretty
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob(""),
	    "module": "ts",
	  }),
	})
'

let watches = tg watch list | from json
assert equal ($watches | length) 1

wait_until { tg watch list | from json | is-empty } "the watch should be removed after its ttl expires"
