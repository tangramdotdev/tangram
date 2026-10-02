use ../lib/test.nu *

# A watched checkin whose root is a single file, rather than a directory, picks up an edit to that file and matches a cold checkin.

let local = server spawn

let path = artifact 'alpha'
let first = tg checkin --no-tokens $path --watch | referent node

# Edit the file and invalidate its node.
'beta' | save --force $path
tg watch touch $path $path

let watched = tg checkin --no-tokens $path --watch | referent node
assert ($first != $watched) "editing the watched file should change the id"

# A cold checkin ignores the watch cache, so it is the ground truth.
let cold = tg checkin --no-tokens $path | referent node
assert ($watched == $cold) "the incremental checkin should equal a cold checkin"

let object = tg get $watched --blobs --depth=inf --no-tokens --pretty
snapshot $object '
	tg.file({
	  "contents": tg.blob("beta"),
	})
'
