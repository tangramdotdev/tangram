use ../lib/test.nu *

# Cancellation rechecks completion when runner control retires after the initial status check.

let server = server spawn --config {
	advanced: { checkpoints: true },
	runner: { process_state_ttl: 0 },
}
let finish_watch = tg checkpoint watch runner.process.finish | from json | get watch
let cancel_watch = tg checkpoint watch process.cancel.runner | from json | get watch
let control_watch = tg checkpoint watch runner.process.control.finished | from json | get watch
let path = artifact { tangram.ts: 'export default () => "done";' }
let spawned = tg build --detach --verbose $path | from json
success (timeout 30s tg checkpoint wait runner.process.finish $finish_watch 0 | complete)
tg index
let batch_watch = tg checkpoint watch index.batch --params '{"finished_process":true}' | from json | get watch
let cancel = job spawn {
	let job_id = job id
	let output = tg cancel $spawned.process $spawned.lease | complete
	$output | job send --tag $job_id 0
}
success (timeout 10s tg checkpoint wait process.cancel.runner $cancel_watch 0 | complete)
tg checkpoint unwatch runner.process.finish $finish_watch
success (timeout 10s tg checkpoint wait index.batch $batch_watch 0 | complete)
success (timeout 10s tg checkpoint wait runner.process.control.finished $control_watch 0 | complete)
tg checkpoint unwatch process.cancel.runner $cancel_watch
let output = job recv --tag $cancel --timeout 5sec
success $output "cancellation must succeed after local control retires while the index is stale"
tg checkpoint unwatch runner.process.control.finished $control_watch
tg checkpoint unwatch index.batch $batch_watch
