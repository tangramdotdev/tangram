use ../../lib/test.nu *

# A shortcut child in a new sandbox derives its own sandbox and process IDs. It runs and finishes before the control server starts its sandbox or process.

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: {
		id: $created.data.id,
		remote: 'default',
		token: $created.token.token,
	},
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Hold the starts on the remote so the child runs while the server has not started its sandbox or process.
let sandbox_start_watch = (
	tg --url $remote.url --token $root_token checkpoint watch sandbox.control.shortcut.received
	| from json
	| get watch
)
let process_start_watch = (
	tg --url $remote.url --token $root_token checkpoint watch process.control.start.received
	| from json
	| get watch
)

# Watch the child's starts and finish on the runner.
let params = { shortcut: 'true' } | to json --raw
let sandbox_sent_watch = (
	tg --url $runner.url checkpoint watch runner.sandbox.control.connect --params $params
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

# The shortcut child connects its sandbox control and its process control.
let output = tg --url $runner.url checkpoint wait --timeout 30s runner.sandbox.control.connect $sandbox_sent_watch 0 | complete
success $output "the shortcut child should connect its sandbox control"
tg --url $runner.url checkpoint continue runner.sandbox.control.connect $sandbox_sent_watch 0
tg --url $runner.url checkpoint unwatch runner.sandbox.control.connect $sandbox_sent_watch

let output = tg --url $runner.url checkpoint wait --timeout 30s runner.process.control.start.sent $process_sent_watch 0 | complete
success $output "the shortcut child should start its process control connection"
tg --url $runner.url checkpoint continue runner.process.control.start.sent $process_sent_watch 0
tg --url $runner.url checkpoint unwatch runner.process.control.start.sent $process_sent_watch

# The remote receives both starts and is held there.
let output = tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sandbox.control.shortcut.received $sandbox_start_watch 0 | complete
success $output "the remote should receive the sandbox start"
let output = tg --url $remote.url --token $root_token checkpoint wait --timeout 30s process.control.start.received $process_start_watch 0 | complete
success $output "the remote should receive the process start"

# The child finishes on the runner while the remote has not started its sandbox or process.
let output = tg --url $runner.url checkpoint wait --timeout 30s runner.process.finish $finish_watch 0 | complete
success $output "the child should finish before the remote starts its sandbox and process"
tg --url $runner.url checkpoint continue runner.process.finish $finish_watch 0
tg --url $runner.url checkpoint unwatch runner.process.finish $finish_watch

# Release the remote.
tg --url $remote.url --token $root_token checkpoint continue sandbox.control.shortcut.received $sandbox_start_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.shortcut.received $sandbox_start_watch
tg --url $remote.url --token $root_token checkpoint continue process.control.start.received $process_start_watch 0
tg --url $remote.url --token $root_token checkpoint unwatch process.control.start.received $process_start_watch

let output = try { job recv --tag $build --timeout 30sec } catch { null }
if $output == null {
	error make { msg: "the run did not complete" }
}
success $output "the run should succeed"
assert ($output.stdout | str contains "42") "the run should return the child output"
