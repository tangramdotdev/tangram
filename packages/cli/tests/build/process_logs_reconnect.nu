use ../../test.nu *

const driver = path self ../lib/log_control.py
if (which python3 | is-empty) {
	skip_test "this test requires python3"
}
let tangram = which tg | where type == external | get path | first
for compaction in [false true] {
	let server = server spawn --config { indexer: { log_compaction: $compaction } }
	let path = artifact { tangram.ts: 'export default function () {}' }
	let id = tg build --detach $path | str trim
	tg wait $id | ignore
	let data = mktemp
	tg get $id | save -f $data
	let output = python3 $driver ($server.directory | path join socket) $tangram $server.url $id $data ($compaction | into string) | complete
	success $output
}
