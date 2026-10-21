use ../lib/test.nu *

# Process log chunks are returned in stream order.

const driver = path self ../lib/log_control.py
if (which python3 | is-empty) {
	skip_test "this test requires python3"
}
let tangram = which tg | where type == external | get path | first
do {
	# Spawn the server with a short live read timeout, because the driver controls the process and never answers live read requests.
	let local = server spawn --config { control: { read_timeout: 0.1 } }
	let path = artifact { tangram.ts: 'export default function () {}' }
	let id = tg build --no-tokens --detach $path | referent node
	tg wait $id | ignore
	let data = mktemp
	tg get $id | save -f $data
	let output = python3 $driver reordered ($local.directory | path join socket) $tangram $local.url $id $data | complete
	success $output
}
