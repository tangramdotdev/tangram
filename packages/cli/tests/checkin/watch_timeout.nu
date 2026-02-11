use ../../test.nu *

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

sleep 2sec

let watches = tg watch list | from json
assert equal ($watches | length) 0
