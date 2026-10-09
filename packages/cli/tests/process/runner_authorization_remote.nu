use ../lib/test.nu *

# A remote capability must fall through to its issuer; runner credentials must not authorize a caller.

let remote_root = random chars
let runner_root = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $remote_root }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $remote_root runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $runner_root }, users: { providers: { insecure: true } } },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $runner.url login --verbose --name alice | from json
let remote_alice = tg --url $remote.url login --verbose --name alice | from json
tg --url $runner.url --token $alice.token remote put default $remote.url
let finish_watch = tg --url $runner.url --token $runner_root checkpoint watch runner.process.finish | from json | get watch
let path = artifact { tangram.ts: 'export default () => tg.file("private output");' }
let spawned = tg --url $remote.url --token $remote_root build --detach --no-tokens --verbose $path | from json
let process = $spawned.process
timeout 30s tg --url $runner.url --token $runner_root checkpoint wait runner.process.finish $finish_watch 0 | ignore

let remote_socket = $remote.url | str replace 'http+unix://' '' | url decode
let runner_socket = $runner.url | str replace 'http+unix://' '' | url decode
let remote_response = http get --unix-socket $remote_socket --headers { Authorization: $'Bearer ($remote_root)' } $'http://localhost/processes/($process)'
let local_response = http get --unix-socket $runner_socket --headers { Authorization: $'Bearer ($runner_root)' } $'http://localhost/processes/($process)?location=remote'

# Get issues a capability with every permission that the caller holds.
for response in [$remote_response $local_response] {
	let body = $response.tokens.local.0 | token body
	assert equal $body.resource $process
	assert equal $body.permissions [process_node process_node_command_objects process_node_error_objects process_node_log_objects process_node_output_objects process_parent process_subtree process_subtree_command_objects process_subtree_error_objects process_subtree_log_objects process_subtree_output_objects]
}

# The owning server must accept its node capability without searching the index for other permissions.
let params = { resource: $process } | to json --raw
let index_watch = tg --url $remote.url --token $remote_root checkpoint watch verification.index --params $params | from json | get watch
let query = { 'tokens[local][0]': $remote_response.tokens.local.0 } | url build-query
let output = http get --max-time 10sec --unix-socket $remote_socket --headers { Authorization: $'Bearer ($remote_alice.token)' } $'http://localhost/processes/($process)?($query)'
assert equal $output.data.status started
tg --url $remote.url --token $remote_root checkpoint unwatch verification.index $index_watch

let headers = { Authorization: $'Bearer ($alice.token)' }
let query = { location: remote } | url build-query
let denied = http get --allow-errors --full --unix-socket $runner_socket --headers $headers $'http://localhost/processes/($process)?($query)'
assert equal $denied.status 404 "the runner's own remote credentials must not authorize the reader"

# Relabeling a foreign token does not make its signature locally valid.
let query = { location: remote, 'tokens[local][0]': $remote_response.tokens.local.0 } | url build-query
let denied = http get --allow-errors --full --unix-socket $runner_socket --headers $headers $'http://localhost/processes/($process)?($query)'
assert equal $denied.status 404

# A remote-only capability must actually reach the owning server.
let response_watch = tg --url $remote.url --token $remote_root checkpoint watch process.control.response.publish --params '{"kind":"get"}' | from json | get watch
let query = { location: remote, 'tokens[remote][0]': $remote_response.tokens.local.0 } | url build-query
let remote_job = job spawn {
	let job_id = job id
	let output = http get --unix-socket $runner_socket --headers $headers $'http://localhost/processes/($process)?($query)'
	$output | job send --tag $job_id 0
}
timeout 10s tg --url $remote.url --token $remote_root checkpoint wait process.control.response.publish $response_watch 0 | ignore

# A local node capability can read the same state without the remote or an index authorization search.
let index_watch = tg --url $runner.url --token $runner_root checkpoint watch verification.index --params $params | from json | get watch
let query = { location: remote, 'tokens[local][0]': $local_response.tokens.local.0 } | url build-query
let output = http get --max-time 10sec --unix-socket $runner_socket --headers $headers $'http://localhost/processes/($process)?($query)'
assert equal $output.location remote
assert equal $output.data.status started
assert ($output.tokens.local? | is-not-empty)
tg --url $runner.url --token $runner_root checkpoint unwatch verification.index $index_watch

tg --url $remote.url --token $remote_root checkpoint continue process.control.response.publish $response_watch 0
tg --url $remote.url --token $remote_root checkpoint unwatch process.control.response.publish $response_watch
let output = job recv --tag $remote_job --timeout 10sec
assert equal $output.location remote
assert ($output.tokens.remote? | is-not-empty)
assert ($output.tokens.local? | is-empty) "an untrusted remote response must not mint local authority"

# The sandbox's runner capabilities must not leak either.
let sandbox = $remote_response.data.sandbox
let denied = http get --allow-errors --full --unix-socket $runner_socket --headers $headers $'http://localhost/sandboxes/($sandbox)?location=remote'
assert equal $denied.status 404

# A get capability must be sufficient to wait through either the runner or local path.
let targets = [
	{ location: remote, reader: $alice.token, root: $runner_root, server: $runner, token: $local_response.tokens.local.0 },
	{ location: local, reader: $remote_alice.token, root: $remote_root, server: $remote, token: $remote_response.tokens.local.0 },
]
let waits = $targets | each { |target|
	let params = { resource: $process, token_resource: $process } | to json --raw
	let index_watch = tg --url $target.server.url --token $target.root checkpoint watch verification.index --params $params | from json | get watch
	let params = { process: $process } | to json --raw
	let attach_watch = tg --url $target.server.url --token $target.root checkpoint watch process.wait.attach --params $params | from json | get watch
	let socket = $target.server.url | str replace 'http+unix://' '' | url decode
	let headers = { Accept: 'text/event-stream', Authorization: $'Bearer ($target.reader)' }
	let query = { lease: $spawned.lease, location: $target.location, 'tokens[local][0]': $target.token } | url build-query
	let wait_job = job spawn {
		let job_id = job id
		let response = http post --raw --max-time 30sec --unix-socket $socket --headers $headers $'http://localhost/processes/($process)/wait?($query)' ''
		$response | job send --tag $job_id 0
	}
	timeout 10s tg --url $target.server.url --token $target.root checkpoint wait process.wait.attach $attach_watch 0 | ignore
	tg --url $target.server.url --token $target.root checkpoint unwatch process.wait.attach $attach_watch
	{ index_watch: $index_watch, job: $wait_job, target: $target }
}

tg --url $runner.url --token $runner_root checkpoint continue runner.process.finish $finish_watch 0
tg --url $runner.url --token $runner_root checkpoint unwatch runner.process.finish $finish_watch
timeout 30s tg --url $remote.url --token $remote_root wait $process | ignore

for wait in $waits {
	let response = job recv --tag $wait.job --timeout 10sec
	let output = $response | into string | lines | where { $in starts-with 'data: ' } | last | str substring 6.. | from json
	assert equal $output.exit 0
	assert ($output.output.value | str contains 'tokens[') "waiting with an output object capability must mint an output capability"
	tg --url $wait.target.server.url --token $wait.target.root checkpoint unwatch verification.index $wait.index_watch
}
