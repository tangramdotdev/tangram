use ../../test.nu *

# Editing a nested file invalidates only its subtree, so a sibling is preserved and the watched checkin matches a cold checkin.

let server = spawn

let path = artifact {
	"a.txt": 'alpha'
	sub: {
		"x.txt": 'one'
	}
}
let first = tg checkin $path --watch

# Edit the nested file and invalidate just that file node.
'two' | save --force ($path | path join 'sub' 'x.txt')
tg watch touch $path ($path | path join 'sub' 'x.txt')

let watched = tg checkin $path --watch
assert ($first != $watched) "editing a nested file should change the id"

# A cold checkin ignores the watch cache, so it is the ground truth.
let cold = tg checkin $path
assert ($watched == $cold) "the incremental checkin should equal a cold checkin"

let object = tg get $watched --blobs --depth=inf --pretty
snapshot $object '
	tg.directory({
	  "a.txt": tg.file({
	    "contents": tg.blob("alpha"),
	  }),
	  "sub": tg.directory({
	    "x.txt": tg.file({
	      "contents": tg.blob("two"),
	    }),
	  }),
	})
'
