use ../../test.nu *

# Removing a file from a watched directory invalidates the directory so the next watched checkin drops it and matches a cold checkin.

let server = spawn

let path = artifact {
	"a.txt": 'alpha'
	"b.txt": 'beta'
}
let first = tg checkin $path --watch

# Remove a file and invalidate the directory node.
rm ($path | path join 'b.txt')
tg watch touch $path $path

let watched = tg checkin $path --watch
assert ($first != $watched) "removing a file should change the id"

# A cold checkin ignores the watch cache, so it is the ground truth.
let cold = tg checkin $path
assert ($watched == $cold) "the incremental checkin should equal a cold checkin"

let object = tg get $watched --blobs --depth=inf --pretty
snapshot $object '
	tg.directory({
	  "a.txt": tg.file({
	    "contents": tg.blob("alpha"),
	  }),
	})
'
