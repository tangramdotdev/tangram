use ../lib/test.nu *

# Indexing a remote inline command must not turn its unused private argument into a process permission.

let remote_root = random chars
let runner_root = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false }
	authentication: { root: { token: $remote_root }, users: { providers: { insecure: true } } }
	roles: [api indexer scheduler]
}
let created = tg --url $remote.url --token $remote_root runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $runner_root }, users: { providers: { insecure: true } } }
	remotes: { default: { token: $created.token.token, url: $remote.url } }
	roles: [api indexer runner]
	runner: { id: $created.data.id, remote: default, token: $created.token.token }
}
let alice = tg --url $runner.url login --verbose --name alice | from json
let reader = tg --url $runner.url login --verbose --name reader | from json
let eve = tg --url $remote.url login --verbose --name eve | from json
let file = tg --url $runner.url --token $alice.token put --no-tokens 'tg.file("private unused argument")' | referent node
tg --url $runner.url --token $runner_root index
failure (tg --url $runner.url --token $reader.token get --bytes $file | complete) "the reader must not have access to the private file."
failure (tg --url $remote.url --token $eve.token get --bytes $file | complete) "the spawning parent must not have access to the private file."

let params = { resource: $file } | to json --raw
let advanced = tg --url $runner.url --token $runner_root checkpoint watch permission_capture.advanced --params $params | from json | get watch
let indexing = tg --url $runner.url --token $runner_root checkpoint watch runner.process.index.started | from json | get watch
let remote_socket = $remote.url | str replace 'http+unix://' '' | url decode
let arg = {
	cached: false
	command: { node: {
		args: [{ kind: value, value: { kind: object, value: $file } }]
		executable: { node: { path: '/bin/true' } }
	} }
	sandbox: {}
	stderr: 'null'
	stdin: 'null'
	stdout: 'null'
} | to json --raw
let spawn = job spawn {
	let job_id = job id
	let events = http post --raw --max-time 30sec --unix-socket $remote_socket --headers { Authorization: $'Bearer ($eve.token)', 'Content-Type': 'application/json' } 'http://localhost/processes/spawn' $arg
	$events | job send --tag $job_id 0
}
let indexed = timeout 30s tg --url $runner.url --token $runner_root checkpoint wait runner.process.index.started $indexing 0 | from json
tg --url $runner.url --token $runner_root checkpoint unwatch runner.process.index.started $indexing
let events = job recv --tag $spawn --timeout 30sec
assert (not ($events | str contains 'event: error')) "the remote must accept the inline command."
let spawned = $events | lines | where { $in starts-with 'data: ' } | last | str substring 6.. | from json
let process = $spawned.process
assert equal $indexed.params.process $process
tg --url $remote.url --token $eve.token wait $process | ignore
let hit = timeout 30s tg --url $runner.url --token $runner_root checkpoint wait permission_capture.advanced $advanced 0 | from json
assert equal $hit.params.process $process
tg --url $runner.url --token $runner_root checkpoint unwatch permission_capture.advanced $advanced
tg --url $runner.url --token $runner_root wait --local --source=index $process | ignore
tg --url $runner.url --token $runner_root index

# The remote indexing checkpoint above excludes the local spawn path, even if checkout denies the private argument.
let socket = $runner.url | str replace 'http+unix://' '' | url decode
let response = http get --unix-socket $socket --headers { Authorization: $'Bearer ($runner_root)' } $'http://localhost/processes/($process)?location=local&source=index'
assert (($response.data.command.node | describe) | str starts-with record)
assert ($response.data.command.node.args | to json --raw | str contains $file)
tg --url $runner.url --token $runner_root grant $reader.user.id process_node_command_objects $process | ignore
tg --url $runner.url --token $runner_root index
success (tg --url $runner.url --token $runner_root get --bytes $file | complete) "the private file must still be stored on the runner."
failure (tg --url $runner.url --token $reader.token get --bytes $file | complete) "an unused inline argument must not confer node access through the process command aspect."
