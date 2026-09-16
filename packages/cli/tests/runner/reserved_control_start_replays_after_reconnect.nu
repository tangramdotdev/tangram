use ../../test.nu *

# A reserved connection whose start the remote has not processed reconnects after a remote restart and replays the start.

let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: {
		id: $created.data.id,
		process_control_pool_size: 1,
		remote: 'default',
		sandbox_control_pool_size: 1,
		token: $created.token.token,
	},
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Hold both starts on the remote.
let sandbox_start_watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.start.started | from json | get watch
let process_start_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.start.started | from json | get watch

let path = artifact {
	"example.tg.ts": '
		export default () => tg.run(child).sandbox(true);
		export const child = () => 42;
	'
}
let spawned = tg --url $local.url build --remote --detach --verbose --user $alice.user.id $"($path)/example.tg.ts" | from json
let process = $spawned.process | split row '?' | first

# The remote receives both starts and is held before indexing either.
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.start.started $sandbox_start_watch 0 | complete
success $output "the remote should receive the sandbox start"
let output = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.start.started $process_start_watch 0 | complete
success $output "the remote should receive the process start"

# Restart the remote. The reserved connections reconnect and replay their unacknowledged starts.
let remote = server restart $remote

let output = timeout 60s tg --url $local.url wait --remote $process | complete
success $output "the run should succeed after the starts replay"
let output = $output.stdout | from json
assert equal $output.exit 0 "the run should finish successfully after the starts replay"
