use ../../test.nu *

# Setting the executable bit on a watched file invalidates that file so the next watched checkin reflects the new mode and matches a cold checkin.

let server = spawn

let path = artifact {
	"run.sh": 'echo hi'
}
let first = tg checkin $path --watch

# Make the file executable and invalidate its node. A metadata change carries no
# new contents, so this exercises the metadata path rather than a content edit.
chmod +x ($path | path join 'run.sh')
tg watch touch $path ($path | path join 'run.sh')

let watched = tg checkin $path --watch
assert ($first != $watched) "setting the executable bit should change the id"

# A cold checkin ignores the watch cache, so it is the ground truth.
let cold = tg checkin $path
assert ($watched == $cold) "the incremental checkin should equal a cold checkin"

let object = tg get $watched --blobs --depth=inf --pretty
snapshot $object '
	tg.directory({
	  "run.sh": tg.file({
	    "contents": tg.blob("echo hi"),
	    "executable": true,
	  }),
	})
'
