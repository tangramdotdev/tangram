use ../../test.nu *

# Retargeting a symlink in a watched directory invalidates it so the next watched checkin reflects the new target and matches a cold checkin.

let server = spawn

let path = artifact {
	"a.txt": 'alpha'
	"b.txt": 'beta'
	link: (symlink 'a.txt')
}
let first = tg checkin $path --watch

# Point the symlink at a different sibling and invalidate its node.
ln -sf 'b.txt' ($path | path join 'link')
tg watch touch $path ($path | path join 'link')

let watched = tg checkin $path --watch
assert ($first != $watched) "retargeting the symlink should change the id"

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
	  "link": tg.symlink({
	    "path": "b.txt",
	  }),
	})
'
