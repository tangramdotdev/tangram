use ../../util/test.nu *

# A run prints its progress and result at the start of separate terminal lines.

const driver = path self ../../util/terminal_output.py

if (which python3 | is-empty) {
	skip_test "this test requires python3"
}

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("Hello, World!");
		}
	',
}

let tangram = which tg | where type == external | get path | first
let output = ^python3 $driver $tangram --no-quiet run --sandbox --no-tokens $path | complete
success $output $output.stdout
let output = $output.stdout | str replace --all --regex '\x1b\[[0-9;]*m' ''
assert ($output =~ 'info pcs_[^\r\n]+\r\n') "the progress message should return the cursor to the first column"
assert ($output =~ '\r\nfil_[^\r\n]+\r\n') "the result should start and end on a new terminal line"
