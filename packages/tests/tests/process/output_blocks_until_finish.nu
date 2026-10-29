use ../../lib/test.nu *

# Requesting the output of a running process blocks until the process finishes and then returns its output.

let local = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: 'export default async function () { return 42; }',
}

# Hold the process before it finishes.
let watch = tg checkpoint watch runner.process.finish | from json | get watch
let process = tg build --no-tokens --detach $path | referent node
tg checkpoint wait runner.process.finish $watch 0 | ignore
assert equal (tg process status --timeout 0 $process | from json) ["started"] "the process should not have finished"

# Request the output while the process is held, wait for the request to reach the server, then let the process finish.
let subscribed = checkpoint watch process.wait.subscribed --params { process: $process }
let output = job spawn {
	let id = job id
	tg output $process | complete | job send --tag $id 0
}
checkpoint pass $subscribed
checkpoint unwatch $subscribed
tg checkpoint continue runner.process.finish $watch 0
tg checkpoint unwatch runner.process.finish $watch

# The output returns the value once the process finishes.
let output = job recv --tag $output --timeout 60sec
success $output
assert (($output.stdout | str trim) == "42") "the output should be the finished process value"
