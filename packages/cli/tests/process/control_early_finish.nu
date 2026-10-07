use ../lib/test.nu *

# Finish is acknowledged before the header, and reconnects preserve the authorization token for the output/error sync.
const driver = path self ../lib/log_control.py
if (which python3 | is-empty) {
	skip_test "this test requires python3"
}
let tangram = which tg | where type == external | get path | first
for source in ['export default () => tg.file("control output");' 'export default () => { throw new Error("control error"); }'] {
	let local = server spawn --config { advanced: { checkpoints: true } }
	let path = artifact { tangram.ts: $source }
	let id = tg build --no-tokens --detach $path | referent node
	tg wait $id | complete | ignore
	let data = mktemp

	# The runner wait can return before control records the finished process data.
	for attempt in 0..100 {
		let process = tg get $id
		if ($process | from json | get status) == 'finished' {
			$process | save -f $data
			break
		}
		sleep 100ms
	}
	assert ((open --raw $data | from json | get status) == 'finished')
	let output = python3 $driver early_finish ($local.directory | path join socket) $tangram $local.url $id $data | complete
	success $output
}
