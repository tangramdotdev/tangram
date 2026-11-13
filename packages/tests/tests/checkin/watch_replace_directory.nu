use ../../util/test.nu *

# Replacing a watched directory that has many files on disk does not hang the next watched checkin, which matches a cold checkin.

let local = server spawn

# Create a directory with more files than the capacity of the watch's event channel, and check it in with a watch.
def create [path: string] {
	mkdir $path
	'export default () => "hello";' | save ($path | path join tangram.ts)
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

# The watched checkin must settle on the id of a cold checkin once the file events are processed.
let cold = tg checkin --no-watch --no-tokens $path | referent node
assert equal $cold $first "an identical directory should have the same id"
wait_until --timeout 30sec {
	let output = timeout 30s tg checkin --no-tokens $path --watch | complete
	$output.exit_code == 0 and ($output.stdout | referent node) == $cold
} "the watched checkin should equal a cold checkin"
