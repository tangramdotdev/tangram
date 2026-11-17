use ../../util/test.nu *

# Replacing a watched directory does not hang the next watched checkin when the file events overflow the watch's channel.

let local = server spawn --config {
	advanced: {
		checkpoints: true
	}
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

# Replace the directory and change its contents, as a branch switch or an unpacked archive does. The watch passes the event checkpoint for the root when it applies the removal of the root, or when it discards its graph because its channel dropped events.
let event = checkpoint watch watch.event --params { path: $path }
rm --recursive $path
create $path
'replacement' | save --force ($path | path join '0.txt')

# The watched checkin must return.
let output = timeout 30s tg checkin --no-tokens $path --watch | complete
assert ($output.exit_code != 124) "the watched checkin should not hang after the directory is replaced"

# Wait for the watch to apply the removal of the root or to discard its graph.
checkpoint wait $event | ignore
checkpoint unwatch $event

# The next watched checkin must return the id of a fresh checkin.
let plain = tg checkin --no-tokens $path --no-watch | referent node
assert ($plain != $first) "the replacement should change the checked in directory"
let output = timeout 30s tg checkin --no-tokens $path --watch | complete
success $output "the watched checkin should succeed"
assert equal ($output.stdout | referent node) $plain "the watched checkin should equal a fresh checkin"
