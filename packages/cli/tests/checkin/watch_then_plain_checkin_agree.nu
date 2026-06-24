use ../../test.nu *

# A --watch checkin notices a real on-disk edit without a touch, and a plain checkin agrees with the watched one.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => "one";'
}

# Check in with --watch to register the watch and capture the initial id.
let first = tg checkin $path --watch

# Edit the file on disk and check in with --watch again. The watch observes the
# edit through a filesystem event, which is delivered asynchronously, so poll
# until a watched checkin reflects it without any tg watch touch.
'export default () => "two";' | save --force ($path | path join tangram.ts)
wait_until { (tg checkin $path --watch) != $first } "the watched checkin should reflect the on-disk edit"
let watched = tg checkin $path --watch

# Check in again without --watch. It should agree with the watched checkin.
let plain = tg checkin $path
assert ($watched == $plain) "the plain checkin should agree with the watched checkin"
assert ($first != $plain) "the plain checkin should not regress to the original contents"

let object = tg get $plain --blobs --depth=inf --pretty
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob("export default () => \"two\";"),
	    "module": "ts",
	  }),
	})
'
