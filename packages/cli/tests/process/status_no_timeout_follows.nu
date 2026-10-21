use ../lib/test.nu *

# Following a process status with --no-timeout streams observed statuses through to completion.

let local = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: 'export default async function () { return 42; }',
}

# Hold the process before it finishes.
let watch = tg checkpoint watch runner.process.finish | from json | get watch
let process = tg build --no-tokens --detach $path | referent node
tg checkpoint wait runner.process.finish $watch 0 | ignore

# Follow the status while the process is held, wait for the request to reach the server, then let the process finish.
let subscribed = checkpoint watch process.status.subscribed --params { process: $process }
let status = job spawn {
	let id = job id
	tg process status --no-timeout $process | complete | job send --tag $id 0
}
checkpoint pass $subscribed
checkpoint unwatch $subscribed
tg checkpoint continue runner.process.finish $watch 0
tg checkpoint unwatch runner.process.finish $watch

let output = job recv --tag $status --timeout 60sec
success $output
let output = $output.stdout | from json
assert (($output | last) == "finished") "the status with follow should end with finished"
