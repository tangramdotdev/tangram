use ../../util/test.nu *

# A directory that is moved out of a watched tree, changed, and moved back is checked in with its new contents.

let local = server spawn

let path = artifact {
	directory: {
		subdirectory: {
			"a.txt": 'a'
		}
	}
	"b.txt": 'b'
}
let first = tg checkin --no-tokens $path --watch | referent node

# Move the directory out of the tree, and wait for the watched checkin to notice.
let directory = $path | path join directory
let moved = $path | path dirname | path join moved
mv $directory $moved
wait_until --timeout 10sec {
	(tg checkin --no-tokens $path --watch | referent node) != $first
} "the watched checkin should notice the move"

# Create a file in the subdirectory while it is outside the tree, and move the directory back.
'c' | save ($moved | path join subdirectory c.txt)
mv $moved $directory

# The watched checkin must settle on the id of a cold checkin.
let cold = tg checkin --no-watch --no-tokens $path | referent node
assert ($cold != $first) "the new file should change the checked in directory"
wait_until --timeout 10sec {
	(tg checkin --no-tokens $path --watch | referent node) == $cold
} "the watched checkin should equal a cold checkin"
