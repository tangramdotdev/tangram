use ../../util/test.nu *

# A caller that keeps a writable file descriptor to a file that it checked in destructively cannot change the checked-in artifact.

let server = server spawn --config { vfs: false }
let directory = mktemp -d
let script = '
	mkdir "$1/directory"
	echo original > "$1/directory/file"
	exec 3>>"$1/directory/file"
	tg checkin --no-tokens --destructive --no-ignore "$1/directory" > "$1/output"
	echo tampered >&3
'
let output = ^sh -euc $script sh $directory | complete
success $output
let id = open --raw ($directory | path join output) | referent node

# The artifact's contents are unchanged.
let checkout = tg checkout $id | str trim
assert equal (open --raw ($checkout | path join file)) "original\n"
