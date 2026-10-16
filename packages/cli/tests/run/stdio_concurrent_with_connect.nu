use ../lib/test.nu *

# A run's stdio requests open concurrently with its connect, locating the process by the connect's nonce before the connect selects it.

let local = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("Hello, World!");
		}
	',
}

# Hold the connect before it selects the process.
let start_watch = tg checkpoint watch process.connect.start | from json | get watch
let retain_watch = tg checkpoint watch process.connect.service.retain | from json | get watch
let run = job spawn {
	let job_id = job id
	tg run --sandbox $path | complete | job send --tag $job_id 0
}
success (timeout 30s tg checkpoint wait process.connect.start $start_watch 0 | complete) "the connect should start"

# The stdio read reaches the server and waits for the process while the connect is held.
success (timeout 30s tg checkpoint wait process.connect.service.retain $retain_watch 0 | complete) "the stdio read should wait for the connect's process"
tg checkpoint unwatch process.connect.service.retain $retain_watch

# Release the connect, and the run prints its output.
tg checkpoint unwatch process.connect.start $start_watch
let output = job recv --tag $run --timeout 30sec
success $output
snapshot $output.stdout '
	Hello, World!

'
