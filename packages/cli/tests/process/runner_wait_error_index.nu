use ../../test.nu *

# Grant preparation waits for the error object's index batch, but a runner wait does not wait for the finished-process batch.

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true, single_process: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let reader = tg login --verbose --name reader | from json
let node_reader = tg login --verbose --name node-reader | from json
let finish_watch = tg --token $root_token checkpoint watch runner.process.finish | from json | get watch
let control_watch = tg --token $root_token checkpoint watch process.control.finish | from json | get watch
let path = artifact {
	tangram.ts: 'export default async () => { console.log("ready"); await tg.sleep(120); };',
}
let spawned = tg --token $root_token spawn --verbose $path | from json
let process = $spawned.process | split row '?' | first
timeout 30s tg --token $root_token process log --no-timeout --length 6 $process | ignore
tg --token $root_token grant $reader.user.id process_node_error $process | ignore
tg --token $root_token grant $node_reader.user.id process_node $process | ignore
tg --token $root_token index

# Hold the error object's index batch before the runner stores the error.
let object_watch = tg --token $root_token checkpoint watch index.batch --params '{"finished_process":false}' | from json | get watch
tg --token $root_token cancel $process $spawned.lease
timeout 10s tg --token $root_token checkpoint wait runner.process.finish $finish_watch 0 | ignore

# Attach the wait before preparing the finished-process grants.
let params = { process: $process } | to json --raw
let attach_watch = tg --token $root_token checkpoint watch process.wait.attach --params $params | from json | get watch
let socket = $server.url | str replace 'http+unix://' '' | url decode
let query = { lease: $spawned.lease } | url build-query
let wait_job = job spawn {
	let job_id = job id
	let output = http post --raw --max-time 30sec --unix-socket $socket --headers { Accept: 'text/event-stream', Authorization: $'Bearer ($node_reader.token)' } $'http://localhost/processes/($process)/wait?($query)' ''
	$output | job send --tag $job_id 0
}
timeout 10s tg --token $root_token checkpoint wait process.wait.attach $attach_watch 0 | ignore
tg --token $root_token checkpoint unwatch process.wait.attach $attach_watch
let process_watch = tg --token $root_token checkpoint watch index.batch --params '{"finished_process":true}' | from json | get watch
tg --token $root_token checkpoint unwatch runner.process.finish $finish_watch
timeout 10s tg --token $root_token checkpoint wait index.batch $object_watch 0 | ignore
let premature = try { job recv --tag $wait_job --timeout 1sec } catch { null }
assert equal $premature null "grant preparation must wait for the error object's index batch before finishing the process"
tg --token $root_token checkpoint unwatch index.batch $object_watch
timeout 10s tg --token $root_token checkpoint wait index.batch $process_watch 0 | ignore
let output = job recv --tag $wait_job --timeout 10sec
let output = $output | lines | where { str starts-with 'data: ' } | last | str substring 6.. | from json
assert equal $output.exit 1
assert (not ($output.error | str contains 'tokens')) "waiting must not mint error capabilities"
timeout 10s tg --token $root_token checkpoint wait process.control.finish $control_watch 0 | ignore

# Authorization must wait for the finished-process batch.
let read_job = job spawn {
	let job_id = job id
	let output = tg --token $reader.token get $output.error | complete
	$output | job send --tag $job_id 0
}
let premature = try { job recv --tag $read_job --timeout 1sec } catch { null }
assert equal $premature null "error authorization must wait for the finished-process batch"
tg --token $root_token checkpoint unwatch index.batch $process_watch
let read = job recv --tag $read_job --timeout 10sec
success $read "error authorization must succeed after indexing without waiting for the control finish handler"
assert ($read.stdout | str contains 'cancellation')
failure (tg --token $node_reader.token get $output.error | complete) "node permission must not grant access to the error"
tg --token $root_token checkpoint unwatch process.control.finish $control_watch
tg --token $root_token index
