use ../../util/test.nu *

# A run's stdio requests open concurrently with its attach, locating the process by the attach's nonce before the attach selects it.

let local = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("Hello, World!");
		}
	',
}

# Hold the attach before it selects the process.
let start_watch = tg checkpoint watch process.attach.start | from json | get watch
let retain_watch = tg checkpoint watch process.attach.service.retain | from json | get watch
let run = job spawn {
	let job_id = job id
	tg run --sandbox $path | complete | job send --tag $job_id 0
}
success (tg checkpoint wait --timeout 30s process.attach.start $start_watch 0 | complete) "the attach should start"

# The stdio read reaches the server and waits for the process while the attach is held.
success (tg checkpoint wait --timeout 30s process.attach.service.retain $retain_watch 0 | complete) "the stdio read should wait for the attach's process"
tg checkpoint unwatch process.attach.service.retain $retain_watch

# Release the attach, and the run prints its output.
tg checkpoint unwatch process.attach.start $start_watch
let output = job recv --tag $run --timeout 30sec
success $output
snapshot $output.stdout '
	Hello, World!

'
