use ../../test.nu *

# A remote runner delivers the finish of an assigned process that exits before process control connects in the connect request.

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with checkpoints enabled.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let control_watch = (
	tg --url $runner.url checkpoint watch runner.process.control.connect
	| from json
	| get watch
)
let exit_watch = (
	tg --url $runner.url checkpoint watch runner.process.exit
	| from json
	| get watch
)
let connect_finish_watch = (
	tg --url $remote.url --token $root_token checkpoint watch process.control.connect.finish
	| from json
	| get watch
)
let finish_request_watch = (
	tg --url $runner.url checkpoint watch runner.process.control.finish.request
	| from json
	| get watch
)

let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf \"%s\" \"$1\" > \"$TANGRAM_OUTPUT\""), "executable": true })'
let file = tg --url $local.url put $artifact | str trim
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $file --arg-string hello | complete
	$output | job send --tag $job_id 0
}

# Hold process control before it connects and let the assigned process run to completion.
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.control.connect $control_watch 0 | complete
success $output "process control should reach the connection checkpoint"
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.exit $exit_watch 0 | complete
success $output "the assigned process should exit before process control connects"
tg --url $runner.url checkpoint continue runner.process.exit $exit_watch 0
tg --url $runner.url checkpoint unwatch runner.process.exit $exit_watch
sleep 100ms
tg --url $runner.url checkpoint continue runner.process.control.connect $control_watch 0
tg --url $runner.url checkpoint unwatch runner.process.control.connect $control_watch

# The remote records the finish while handling the connect request.
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.connect.finish $connect_finish_watch 0 | complete
success $output "the connect request should carry the finished process"
tg --url $remote.url --token $root_token checkpoint continue process.control.connect.finish $connect_finish_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch process.control.connect.finish $connect_finish_watch

let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the build did not complete after process control connected" }
}
success $output "the build should complete after process control connects"
assert ($output.stdout | str contains 'fil_')

# The runner never sends a separate finish request.
let output = timeout 2s tg --url $runner.url checkpoint wait runner.process.control.finish.request $finish_request_watch 0 | complete
failure $output "the runner should not send a finish request for a process that finished before connecting"
tg --url $runner.url checkpoint unwatch runner.process.control.finish.request $finish_request_watch
