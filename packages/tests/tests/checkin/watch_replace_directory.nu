use ../../util/test.nu *

# Replacing a watched directory that has many files does not hang the next watched checkin.

let local = server spawn

# Create a directory with enough files that replacing it queues thousands of file events, and check it in with a watch.
def create [path: string] {
	mkdir $path
	for i in 0..<2000 {
		$'($i)' | save ($path | path join $'($i).txt')
	}
}
let path = mktemp --directory | path expand | path join directory
create $path
let first = tg checkin --no-tokens $path --watch | referent node

# Replace the directory with an identical one, as a branch switch or an unpacked archive does.
rm --recursive $path
create $path

# The watched checkin must return.
let output = timeout 30s tg checkin --no-tokens $path --watch | complete
assert ($output.exit_code != 124) "the watched checkin should not hang after the directory is replaced"

# The watched checkin must settle on the directory's id once the file events are processed.
wait_until --timeout 30sec {
	let output = timeout 30s tg checkin --no-tokens $path --watch | complete
	$output.exit_code == 0 and ($output.stdout | referent node) == $first
} "the watched checkin should return the directory's id"
