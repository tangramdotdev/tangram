use ../../util/test.nu *

# A file created in a subdirectory of a directory that was renamed and renamed back is observed by a watched checkin.

let local = server spawn --config {
	advanced: {
		checkpoints: true
	}
}

# Rename a directory, and wait for the watch to apply the rename's last event. Inotify reports the old path, the new path, and then both paths, while FSEvents reports the old path and then the new path.
def rename [from: string, to: string] {
	let event = if $nu.os-info.name == 'linux' {
		checkpoint watch watch.event --params { path: $from }
	} else {
		checkpoint watch watch.event --params { path: $to }
	}
	mv $from $to
	if $nu.os-info.name == 'linux' {
		checkpoint pass $event 0 | ignore
		checkpoint wait $event 1 | ignore
	} else {
		checkpoint wait $event | ignore
	}
	checkpoint unwatch $event
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

# Rename the directory, and check that the watched checkin notices.
let directory = $path | path join directory
let renamed = $path | path join renamed
rename $directory $renamed
let cold = tg checkin --no-watch --no-tokens $path | referent node
let watched = tg checkin --no-tokens $path --watch | referent node
assert ($watched == $cold) "the watched checkin should notice the rename"

# Rename the directory back, and check that the watched checkin notices.
rename $renamed $directory
let watched = tg checkin --no-tokens $path --watch | referent node
assert ($watched == $first) "the watched checkin should notice the rename back"

# Create a file in the subdirectory, and wait for the watch to apply the creation.
let file = $directory | path join subdirectory c.txt
let event = checkpoint watch watch.event --params { path: $file }
'c' | save $file
checkpoint wait $event | ignore
checkpoint unwatch $event

# The next watched checkin must return the id of a cold checkin.
let cold = tg checkin --no-watch --no-tokens $path | referent node
assert ($cold != $first) "the new file should change the checked in directory"
let watched = tg checkin --no-tokens $path --watch | referent node
assert ($watched == $cold) "the watched checkin should equal a cold checkin"
