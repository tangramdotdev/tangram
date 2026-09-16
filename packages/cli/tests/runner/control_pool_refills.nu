use ../../test.nu *

# The control pools reserve a replacement connection on the refill interval after one is taken.

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
		connection_pool_refill_interval: 0.1,
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

let sandbox_reserved_watch = tg --url $runner.url checkpoint watch runner.sandbox.control.reserved | from json | get watch
let process_reserved_watch = tg --url $runner.url checkpoint watch runner.process.control.reserved | from json | get watch

# A shortcut child takes both reserved connections.
let path = artifact {
	"example.tg.ts": '
		export default () => tg.run(child).sandbox(true);
		export const child = () => 42;
	'
}
let output = tg --url $local.url run --no-tty --remote --user $alice.user.id $"($path)/example.tg.ts" | complete
success $output "the run should succeed with pooled control connections"

# Each pool reserves a replacement on the refill interval.
let output = timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.control.reserved $sandbox_reserved_watch 0 | complete
success $output "the sandbox control pool should refill after a take"
tg --url $runner.url checkpoint continue runner.sandbox.control.reserved $sandbox_reserved_watch 0
tg --url $runner.url checkpoint unwatch runner.sandbox.control.reserved $sandbox_reserved_watch
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.control.reserved $process_reserved_watch 0 | complete
success $output "the process control pool should refill after a take"
tg --url $runner.url checkpoint continue runner.process.control.reserved $process_reserved_watch 0
tg --url $runner.url checkpoint unwatch runner.process.control.reserved $process_reserved_watch
