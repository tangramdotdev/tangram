use ../../test.nu *

# Renaming a file in a watched directory invalidates the directory so the next watched checkin reflects the new name and matches a cold checkin.

let server = spawn

let path = artifact {
	"a.txt": 'alpha'
}
let first = tg checkin $path --watch

# Rename the file and invalidate the directory node.
mv ($path | path join 'a.txt') ($path | path join 'b.txt')
tg watch touch $path $path

let watched = tg checkin $path --watch
assert ($first != $watched) "renaming a file should change the id"

# A cold checkin ignores the watch cache, so it is the ground truth.
let cold = tg checkin $path
assert ($watched == $cold) "the incremental checkin should equal a cold checkin"

let object = tg get $watched --blobs --depth=inf --pretty
snapshot $object '
	tg.directory({
	  "b.txt": tg.file({
	    "contents": tg.blob("alpha"),
	  }),
	})
'
