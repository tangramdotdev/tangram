use ../lib/test.nu *

# Renaming a file in a watched directory invalidates the directory so the next watched checkin reflects the new name and matches a cold checkin.

let local = server spawn

let path = artifact {
	"a.txt": 'alpha'
}
let first = tg checkin --no-tokens $path --watch | referent node

# Rename the file and invalidate the directory node.
mv ($path | path join 'a.txt') ($path | path join 'b.txt')
tg watch touch $path $path

let watched = tg checkin --no-tokens $path --watch | referent node
assert ($first != $watched) "renaming a file should change the id"

# A cold checkin ignores the watch cache, so it is the ground truth.
let cold = tg checkin --no-tokens $path | referent node
assert ($watched == $cold) "the incremental checkin should equal a cold checkin"

let object = tg get $watched --blobs --depth=inf --no-tokens --pretty
snapshot $object '
	tg.directory({
	  "b.txt": tg.file({
	    "contents": tg.blob("alpha"),
	  }),
	})
'
