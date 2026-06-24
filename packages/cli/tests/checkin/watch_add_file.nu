use ../../test.nu *

# Adding a file to a watched directory invalidates the directory so the next watched checkin includes it and matches a cold checkin.

let server = spawn

let path = artifact {
	"a.txt": 'alpha'
}
let first = tg checkin $path --watch

# Add a new file and invalidate the directory node.
'beta' | save ($path | path join 'b.txt')
tg watch touch $path $path

let watched = tg checkin $path --watch
assert ($first != $watched) "adding a file should change the id"

# A cold checkin ignores the watch cache, so it is the ground truth.
let cold = tg checkin $path
assert ($watched == $cold) "the incremental checkin should equal a cold checkin"

let object = tg get $watched --blobs --depth=inf --pretty
snapshot $object '
	tg.directory({
	  "a.txt": tg.file({
	    "contents": tg.blob("alpha"),
	  }),
	  "b.txt": tg.file({
	    "contents": tg.blob("beta"),
	  }),
	})
'
