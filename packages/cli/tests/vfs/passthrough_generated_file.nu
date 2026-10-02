use ../lib/test.nu *
use ../lib/vfs.nu

# A generated file without a backing descriptor falls back to regular FUSE I/O even when passthrough is required.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let server_path = mktemp --directory
let local = server spawn --directory $server_path --config {
	vfs: {
		kind: 'fuse'
		passthrough: 'required'
	}
}
vfs assert_mounted $server_path

# Probe passthrough with a checked-in file that already has a physical backing file.
let checked_in = tg checkin --no-tokens (artifact { probe: 'probe' }) | referent node
let probe = vfs root $server_path $checked_in | path join probe
if (^cat $probe | complete).exit_code != 0 {
	skip_test 'this test requires FUSE passthrough support'
}

let generated = tg put --no-tokens 'tg.file("contents")' | referent node
let path = vfs root $server_path $generated
let output = ^cat $path | complete
assert ($output.exit_code == 0) $'expected the generated file to open, got: ($output.stderr | str trim)'
assert equal $output.stdout 'contents'
