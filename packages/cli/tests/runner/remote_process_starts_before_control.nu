use ../../test.nu *

# A remote runner starts an assigned process without waiting for process control to connect.

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with checkpoints enabled.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.runner.id, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Watch the process state, control connection, and start.
let control_watch = (
	tg --url $runner.url checkpoint watch runner.process.control.connect
	| from json
	| get watch
)
let start_watch = (
	tg --url $runner.url checkpoint watch runner.process.start
	| from json
	| get watch
)
let state_watch = (
	tg --url $runner.url checkpoint watch runner.process.state.inserted
	| from json
	| get watch
)

let path = artifact {
	tangram.ts: 'export default () => 42',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

# Hold process control before it connects. The assigned process must load its remote command and start.
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.state.inserted $state_watch 0 | complete
success $output "the assigned process state should be stored before process control connects"
tg --url $runner.url checkpoint continue runner.process.state.inserted $state_watch 0
tg --url $runner.url checkpoint unwatch runner.process.state.inserted $state_watch

let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.control.connect $control_watch 0 | complete
success $output "process control should reach the connection checkpoint"
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | complete
success $output "the assigned process should start before process control connects"

tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch
tg --url $runner.url checkpoint continue runner.process.control.connect $control_watch 0
tg --url $runner.url checkpoint unwatch runner.process.control.connect $control_watch

let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the build did not complete after process control connected" }
}
success $output "the build should complete after process control connects"
