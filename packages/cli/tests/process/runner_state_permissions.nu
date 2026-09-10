use ../../test.nu *

# Runner-backed reads preserve process and output permissions before completion reaches the index.

let root_token = random chars
let server = server spawn --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let owner = tg login --verbose --name owner | from json
let reader = tg login --verbose --name reader | from json
let outsider = tg login --verbose --name outsider | from json
let runner_finish_watch = tg --token $root_token checkpoint watch runner.process.finish | from json | get watch
let finish_watch = tg --token $root_token checkpoint watch process.control.finish | from json | get watch
let path = artifact { tangram.ts: 'export default () => tg.file("output");' }
let spawned = tg --token $owner.token build --detach --verbose $path | from json
let process = $spawned.process | split row '?' | first
timeout 30s tg --token $root_token checkpoint wait runner.process.finish $runner_finish_watch 0 | ignore
tg --token $owner.token grant $reader.user.id process_node $process | ignore

success (tg --token $reader.token process get $process | complete)
success (tg --token $reader.token process children $process | complete)
let status = tg --token $reader.token process status $process | from json
assert equal $status [started]

failure (tg --token $outsider.token process get $process | complete)
failure (tg --token $outsider.token process status $process | complete)
failure (tg --token $outsider.token process children $process | complete)
failure (tg --token $outsider.token wait $process | complete)

# Attach the node reader while the process is active, then finish without publishing to the index.
let params = { process: $process } | to json --raw
let attach_watch = tg --token $root_token checkpoint watch process.wait.attach --params $params | from json | get watch
let socket = $server.url | str replace 'http+unix://' '' | url decode
let query = { location: 'local(hint)', timeout: 0 } | url build-query
for endpoint in ['' '/status' '/children'] {
	let output = http get --full --max-time 10sec --unix-socket $socket --headers { Authorization: $'Bearer ($reader.token)' } $'http://localhost/processes/($process)($endpoint)?($query)'
	assert equal $output.status 200 "a region hint must not prevent a local runner read"
}
let query = { lease: $spawned.lease, location: 'local(hint)' } | url build-query
let wait_job = job spawn {
	let job_id = job id
	let output = http post --raw --max-time 30sec --unix-socket $socket --headers { Accept: 'text/event-stream', Authorization: $'Bearer ($reader.token)' } $'http://localhost/processes/($process)/wait?($query)' ''
	$output | job send --tag $job_id 0
}
timeout 10s tg --token $root_token checkpoint wait process.wait.attach $attach_watch 0 | ignore
tg --token $root_token checkpoint unwatch process.wait.attach $attach_watch
tg --token $root_token checkpoint unwatch runner.process.finish $runner_finish_watch
timeout 30s tg --token $root_token checkpoint wait process.control.finish $finish_watch 0 | ignore
let result = job recv --tag $wait_job --timeout 10sec
let result = $result | lines | where { str starts-with 'data: ' } | last | str substring 6.. | from json
assert equal $result.exit 0
assert (not ($result.output.value | str contains 'tokens')) "a node reader must not receive an output capability"
failure (tg --token $reader.token cat $result.output.value | complete) "a node reader must not read the output"

# New waits use the normal path once completion is published.
tg --token $root_token checkpoint continue process.control.finish $finish_watch 0
tg --token $root_token checkpoint unwatch process.control.finish $finish_watch
tg --token $owner.token grant $reader.user.id process_node_output $process | ignore
let result = timeout 10s tg --token $reader.token wait $process | from json
assert (not ($result.output.value | str contains 'tokens')) "waiting must not mint an output capability even for an output reader"
