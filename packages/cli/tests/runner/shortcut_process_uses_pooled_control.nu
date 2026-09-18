use ../../test.nu *

# A shortcut child in a new sandbox takes reserved sandbox and process control connections from the pools. It runs and finishes before the control server starts either of them.

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with sandbox and process control pools.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: {
		id: $created.data.id,
		process_control_connection_pool_size: 1,
		remote: 'default',
		sandbox_control_connection_pool_size: 1,
		token: $created.token.token,
	},
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Hold the starts on the remote so the child runs while the server has not started its sandbox or process.
let sandbox_start_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sandbox.control.create.received
	| from json
	| get watch
)
let process_start_watch = (
	tg --url $remote.url --token $root_token checkpoint watch process.control.start.received
	| from json
	| get watch
)

# Watch the pooled starts and the child finish on the runner.
let sandbox_sent_watch = (
	tg --url $runner.url checkpoint watch runner.sandbox.control.create.sent
	| from json
	| get watch
)
let process_sent_watch = (
	tg --url $runner.url checkpoint watch runner.process.control.start.sent
	| from json
	| get watch
)
let finish_watch = (
	tg --url $runner.url checkpoint watch runner.process.finish
	| from json
	| get watch
)

let path = artifact {
	"example.tg.ts": '
		export default async () => tg.run(await tg.file({ contents: "#!/bin/sh\nprintf 42", executable: true })).sandbox(true);
	'
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url run --no-tty --remote --user $alice.user.id $"($path)/example.tg.ts" | complete
	$output | job send --tag $job_id 0
}

# The shortcut child starts the pooled sandbox and process connections.
let output = timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.control.create.sent $sandbox_sent_watch 0 | complete
success $output "the shortcut child should start a pooled sandbox control connection"
tg --url $runner.url checkpoint continue runner.sandbox.control.create.sent $sandbox_sent_watch 0
tg --url $runner.url checkpoint unwatch runner.sandbox.control.create.sent $sandbox_sent_watch

let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.control.start.sent $process_sent_watch 0 | complete
success $output "the shortcut child should start a pooled process control connection"
tg --url $runner.url checkpoint continue runner.process.control.start.sent $process_sent_watch 0
tg --url $runner.url checkpoint unwatch runner.process.control.start.sent $process_sent_watch

# The remote receives both starts and is held there.
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.create.received $sandbox_start_watch 0 | complete
success $output "the remote should receive the sandbox start"
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.start.received $process_start_watch 0 | complete
success $output "the remote should receive the process start"

# The child finishes on the runner while the remote has not started its sandbox or process.
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.finish $finish_watch 0 | complete
success $output "the child should finish before the remote starts its sandbox and process"
tg --url $runner.url checkpoint continue runner.process.finish $finish_watch 0
tg --url $runner.url checkpoint unwatch runner.process.finish $finish_watch

# Release the remote.
tg --url $remote.url --token $root_token checkpoint continue sandbox.control.create.received $sandbox_start_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.create.received $sandbox_start_watch
tg --url $remote.url --token $root_token checkpoint continue process.control.start.received $process_start_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch process.control.start.received $process_start_watch

let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the run did not complete" }
}
success $output "the run should succeed with pooled control connections"
assert ($output.stdout | str contains "42") "the run should return the child output"
