use ../lib/test.nu *

# A --watch checkin notices a real on-disk edit without a touch, and a plain checkin agrees with the watched one.

let local = server spawn --config {
	tracing: { filter: 'tangram_server_watch=trace', stderr_format: 'json' },
}

let path = artifact {
	tangram.ts: 'export default () => "one";'
}

# Check in with --watch to register the watch and capture the initial id.
let first = tg checkin --no-tokens $path --watch | referent node

# Edit the file on disk, wait for the watch to remove the file's node in response to the filesystem event, and check in with --watch again.
'export default () => "two";' | save --force ($path | path join tangram.ts)
let removed = '"message":"removed","path":"Some(\"' + ($path | path join tangram.ts) + '\")"'
wait_for_log $local $removed
let watched = tg checkin --no-tokens $path --watch | referent node
assert ($watched != $first) "the watched checkin should reflect the on-disk edit"

# Check in again without --watch. It should agree with the watched checkin.
let plain = tg checkin --no-tokens $path | referent node
assert ($watched == $plain) "the plain checkin should agree with the watched checkin"
assert ($first != $plain) "the plain checkin should not regress to the original contents"

let object = tg get $plain --blobs --depth=inf --no-tokens --pretty
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("export default () => \"two\";"),
	    "module": "typescript",
	  }),
	})
'
