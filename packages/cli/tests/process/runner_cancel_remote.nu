use ../../test.nu *

# Cancellation must reach the owning remote even when this server holds the runner state.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let path = artifact {
	tangram.ts: 'export default async () => { console.log("ready"); await tg.sleep(120); };',
}
let spawned = tg --url $remote.url --token $root_token spawn --verbose $path | from json
let process = $spawned.process
let log = timeout 30s tg --url $remote.url --token $root_token process log --no-timeout --length 6 $process | str trim
assert equal $log ready

# A runner read confirms the state is present without changing its logical location.
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let output = http get --max-time 10sec --unix-socket $socket $'http://localhost/processes/($process)?location=remote'
assert equal $output.location remote
assert equal $output.data.status started

# The same index entry must not be selected for a local AcquireLease request.
tg --url $runner.url index
let cached = timeout 10s tg --url $runner.url spawn --cached --local --verbose $path | complete
failure $cached "a running remote process must not be treated as a local cache candidate"
assert ($cached.stderr | str contains "expected a process") $cached.stderr

# An explicit local request must not release the remote lease.
let query = { lease: $spawned.lease, location: local } | url build-query
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket $'http://localhost/processes/($process)/cancel?($query)' ''
assert equal $output.status 404

let params = { kind: release_lease, process: $process } | to json --raw
let response_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.response.publish --params $params | from json | get watch
let cancel_job = job spawn {
	let job_id = job id
	# Use the endpoint directly because the CLI resolves a location before cancelling.
	let query = { lease: $spawned.lease } | url build-query
	let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket $'http://localhost/processes/($process)/cancel?($query)' ''
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $remote.url --token $root_token checkpoint wait process.control.response.publish $response_watch 0 | ignore
tg --url $remote.url --token $root_token checkpoint continue process.control.response.publish $response_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch process.control.response.publish $response_watch
let output = job recv --tag $cancel_job --timeout 10sec
assert equal $output.status 200 "cancellation without a location must reach the remote control connection"
assert ($output.body | from json | get released)
success (tg --url $runner.url cancel --remote $process $spawned.lease | complete) "explicit remote cancellation must still succeed"
timeout 30s tg --url $remote.url --token $root_token wait $process | ignore
tg --url $runner.url index
let query = { lease: $spawned.lease, location: local } | url build-query
let output = http post --full --allow-errors --max-time 10sec --unix-socket $socket $'http://localhost/processes/($process)/cancel?($query)' ''
assert equal $output.status 404 "finishing the process must preserve its remote location"
