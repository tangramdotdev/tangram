use ../../util/test.nu *

# Replacing a watched directory does not hang the next watched checkin when the file events overflow the watch's channel.

let local = server spawn --config {
	watch: {
		channel_capacity: 16
	}
}

# Create a directory with more files than the watch's channel holds, and check it in with a watch.
def create [path: string] {
	mkdir $path
	for i in 0..<100 {
		$'($i)' | save ($path | path join $'($i).txt')
	}
}
let path = mktemp --directory | path expand | path join directory
create $path
let first = tg checkin --no-tokens $path --watch | referent node

# Replace the directory and change its contents, as a branch switch or an unpacked archive does.
rm --recursive $path
create $path
'replacement' | save --force ($path | path join '0.txt')

# The watched checkin must return.
let output = timeout 30s tg checkin --no-tokens $path --watch | complete
assert ($output.exit_code != 124) "the watched checkin should not hang after the directory is replaced"

# The watched checkin must settle on the id of a fresh checkin once the file events are processed.
let plain = tg checkin --no-tokens $path --no-watch | referent node
assert ($plain != $first) "the replacement should change the checked in directory"
wait_until --timeout 30sec {
	let output = timeout 30s tg checkin --no-tokens $path --watch | complete
	$output.exit_code == 0 and ($output.stdout | referent node) == $plain
} "the watched checkin should equal a fresh checkin"
