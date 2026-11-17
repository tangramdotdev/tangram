use ../../util/test.nu *

# A directory that is moved out of a watched tree, changed, and moved back is checked in with its new contents.

let local = server spawn --config {
	advanced: {
		checkpoints: true
	}
}

let path = artifact {
	directory: {
		subdirectory: {
			"a.txt": 'a'
		}
	}
	"b.txt": 'b'
}
let first = tg checkin --no-tokens $path --watch | referent node

# Move the directory out of the tree, and wait for the watch to apply the move.
let directory = $path | path join directory
let moved = $path | path dirname | path join moved
let event = checkpoint watch watch.event --params { path: $directory }
mv $directory $moved
checkpoint wait $event | ignore
checkpoint unwatch $event
let watched = tg checkin --no-tokens $path --watch | referent node
assert ($watched != $first) "the watched checkin should notice the move"

# Rewrite a file in the tree with its contents, and wait for the watch to apply the write. The watch applies events in the order that the file system reports them, and the file system reports every event of the move out, such as the second rename event that inotify reports for the moved directory itself, before the events of the write, so that no event of the move out is applied after this.
let file = $path | path join b.txt
let event = checkpoint watch watch.event --params { path: $file }
'b' | save --force $file
checkpoint wait $event | ignore
checkpoint unwatch $event

# Create a file in the subdirectory while it is outside the tree, move the directory back, and wait for the watch to apply the move. The watch for the move back starts after every event of the move out was applied, so only an event of the move back can hit it.
'c' | save ($moved | path join subdirectory c.txt)
let event = checkpoint watch watch.event --params { path: $directory }
mv $moved $directory
checkpoint wait $event | ignore
checkpoint unwatch $event

# The next watched checkin must return the id of a cold checkin.
let cold = tg checkin --no-watch --no-tokens $path | referent node
assert ($cold != $first) "the new file should change the checked in directory"
let watched = tg checkin --no-tokens $path --watch | referent node
assert ($watched == $cold) "the watched checkin should equal a cold checkin"
