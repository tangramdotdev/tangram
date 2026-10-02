use ../lib/test.nu *

# Runner-backed reads preserve process and output permissions before completion reaches the index.

let root_token = random chars
let local = server spawn --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json
let carol = tg login --verbose --name carol | from json
let runner_finish_watch = tg --token $root_token checkpoint watch runner.process.finish | from json | get watch
let finish_watch = tg --token $root_token checkpoint watch process.control.finish | from json | get watch
let path = artifact { tangram.ts: 'export default () => tg.file("output");' }
let spawned = tg --token $alice.token build --detach --no-tokens --verbose $path | from json
let process = $spawned.process
timeout 30s tg --token $root_token checkpoint wait runner.process.finish $runner_finish_watch 0 | ignore
tg --token $alice.token grant $bob.user.id process_node $process | ignore

success (tg --token $bob.token process get $process | complete)
success (tg --token $bob.token process children $process | complete)
let status = tg --token $bob.token process status $process | from json
assert equal $status [started]

let output = tg --token $carol.token process get $process | complete
failure $output "an unrelated principal must not get the process"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the process
	   id = pcs_0000000000000000000000000000
	-> failed to get the process

'
let output = tg --token $carol.token process status $process | complete
failure $output "an unrelated principal must not read the process status"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the process status
	   id = pcs_0000000000000000000000000000
	-> failed to get the process

'
let output = tg --token $carol.token process children $process | complete
failure $output "an unrelated principal must not read the process children"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to get the process children
	   id = pcs_0000000000000000000000000000
	-> failed to get the process

'
let output = tg --token $carol.token wait $process | complete
failure $output "an unrelated principal must not wait for the process"
snapshot --normalize $output.stderr '
	error an error occurred
	-> failed to wait for the process
	   id = pcs_0000000000000000000000000000
	-> failed to find the process

'

# Attach the node reader while the process is active, then finish without publishing to the index.
let params = { process: $process } | to json --raw
let attach_watch = tg --token $root_token checkpoint watch process.wait.attach --params $params | from json | get watch
let socket = $local.url | str replace 'http+unix://' '' | url decode
let query = { location: 'local(hint)', timeout: 0 } | url build-query
for endpoint in ['' '/status' '/children'] {
	let output = http get --full --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($bob.token)' } $'http://localhost/processes/($process)($endpoint)?($query)'
	assert equal $output.status 200 "a region hint must not prevent a local runner read"
}
let query = { lease: $spawned.lease, location: 'local(hint)' } | url build-query
let wait_job = job spawn {
	let job_id = job id
	let output = http post --raw --max-time 30sec --unix-socket $socket --headers { Accept: 'text/event-stream', Authorization: $'Bearer ($bob.token)' } $'http://localhost/processes/($process)/wait?($query)' ''
	$output | job send --tag $job_id 0
}
timeout 10s tg --token $root_token checkpoint wait process.wait.attach $attach_watch 0 | ignore
tg --token $root_token checkpoint unwatch process.wait.attach $attach_watch
tg --token $root_token checkpoint unwatch runner.process.finish $runner_finish_watch
timeout 30s tg --token $root_token checkpoint wait process.control.finish $finish_watch 0 | ignore
let result = job recv --tag $wait_job --timeout 10sec
let result = $result | lines | where { str starts-with 'data: ' } | last | str substring 6.. | from json
assert equal $result.exit 0
assert (not ($result.output.value | str contains 'tokens[')) "a node reader must not receive an output capability"
let output = tg --token $bob.token cat $result.output.value | complete
failure $output "a node reader must not read the output"
snapshot $output.stderr '
	error an error occurred
	-> failed to get file contents
	-> failed to load the object

'

# An output reader can receive the output capability and read the result before completion is indexed.
tg --token $alice.token grant $bob.user.id process_node_output_objects $process | ignore
let result = timeout 10s tg --token $bob.token wait $process | from json
assert equal $result.exit 0
assert ($result.output.value | str contains 'tokens[') "an output reader should retain the output capability"
let output = tg --token $bob.token cat $result.output.value | complete
success $output "an output reader must read the result"
assert equal $output.stdout 'output'
tg --token $root_token checkpoint continue process.control.finish $finish_watch 0
tg --token $root_token checkpoint unwatch process.control.finish $finish_watch
