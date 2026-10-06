use ../lib/test.nu *

# Editing a default-ignored file under a watched directory leaves the id unchanged even after the directory is invalidated and re-read.

let local = server spawn

let path = artifact {
	tangram.ts: ''
	.git: {
		config: 'initial'
	}
}
let first = tg checkin --no-tokens $path --watch | referent node

# Edit an ignored file and invalidate the directory node to force a re-read.
'changed' | save --force ($path | path join '.git' 'config')
tg watch touch $path $path

let watched = tg checkin --no-tokens $path --watch | referent node
assert ($first == $watched) "editing an ignored file should not change the id"

# A cold checkin ignores the watch cache, so it is the ground truth.
let cold = tg checkin --no-tokens $path | referent node
assert ($watched == $cold) "the incremental checkin should equal a cold checkin"

let object = tg get $watched --blobs --depth=inf --no-tokens --pretty
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": tg.blob(""),
	    "module": "typescript",
	  }),
	})
'
