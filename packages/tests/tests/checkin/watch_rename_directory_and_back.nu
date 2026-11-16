use ../../util/test.nu *

# A file created in a subdirectory of a directory that was renamed and renamed back is observed by a watched checkin.

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

# Rename the directory, and wait for the watched checkin to notice.
let directory = $path | path join directory
let renamed = $path | path join renamed
mv $directory $renamed
let cold = tg checkin --no-watch --no-tokens $path | referent node
wait_until --timeout 10sec {
	(tg checkin --no-tokens $path --watch | referent node) == $cold
} "the watched checkin should notice the rename"

# Rename the directory back, and wait for the watched checkin to notice.
mv $renamed $directory
wait_until --timeout 10sec {
	(tg checkin --no-tokens $path --watch | referent node) == $first
} "the watched checkin should notice the rename back"

# Create a file in the subdirectory.
'c' | save ($directory | path join subdirectory c.txt)

# The watched checkin must settle on the id of a cold checkin.
let cold = tg checkin --no-watch --no-tokens $path | referent node
assert ($cold != $first) "the new file should change the checked in directory"
wait_until --timeout 10sec {
	(tg checkin --no-tokens $path --watch | referent node) == $cold
} "the watched checkin should equal a cold checkin"
