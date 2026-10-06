use ../lib/test.nu *

# A process log reader can follow output as the log grows.

const driver = path self ../lib/log_control.py
if (which python3 | is-empty) {
	skip_test "this test requires python3"
}
let tangram = which tg | where type == external | get path | first
do {
	let local = server spawn
	let path = artifact { tangram.ts: 'export default function () {}' }
	let id = tg build --no-tokens --detach $path | referent node
	tg wait $id | ignore
	let data = mktemp
	tg get $id | save -f $data
	let output = python3 $driver growing ($local.directory | path join socket) $tangram $local.url $id $data | complete
	success $output
}
