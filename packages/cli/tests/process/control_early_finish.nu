use ../lib/test.nu *

# Finish is acknowledged before the header, and every header carries a sync.
const driver = path self ../lib/log_control.py
if (which python3 | is-empty) {
	skip_test "this test requires python3"
}
let tangram = which tg | where type == external | get path | first
for source in ['export default () => tg.file("control output");' 'export default () => { throw new Error("control error"); }'] {
	let local = server spawn --config { advanced: { checkpoints: true } }
	let path = artifact { tangram.ts: $source }
	let watch = tg checkpoint watch process.control.finish.submitted | from json | get watch
	let id = tg build --no-tokens --detach $path | referent node

	# Wait for control to record the finished process data.
	success (tg checkpoint wait --timeout 30s process.control.finish.submitted $watch 0 | complete) "control must record the finished process"
	tg checkpoint unwatch process.control.finish.submitted $watch
	let data = mktemp
	tg get $id | save -f $data
	assert ((open --raw $data | from json | get status) == 'finished')
	let output = python3 $driver early_finish ($local.directory | path join socket) $tangram $local.url $id $data | complete
	success $output
}
