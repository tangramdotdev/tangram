use ../../util/test.nu *

# A scheduled process starts in a pooled sandbox before sandbox control connects.

let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, sandbox_pool_size: 1, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let connect_watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.connect | from json | get watch
let pool_watch = tg --url $runner.url checkpoint watch runner.sandbox.pool.take | from json | get watch
let state_watch = tg --url $runner.url checkpoint watch runner.sandbox.state.inserted | from json | get watch
let start_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
let path = artifact {
	tangram.ts: 'export default () => 42',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

# The assigned identity activates an IDless physical sandbox without waiting for control.
success (tg --url $runner.url checkpoint wait --timeout 30s runner.sandbox.pool.take $pool_watch 0 | complete) "the scheduled sandbox should take a pooled sandbox"
tg --url $runner.url checkpoint unwatch runner.sandbox.pool.take $pool_watch
success (tg --url $remote.url --token $root_token checkpoint wait --timeout 30s sandbox.control.connect $connect_watch 0 | complete) "sandbox control should begin connecting"
success (tg --url $runner.url checkpoint wait --timeout 30s runner.sandbox.state.inserted $state_watch 0 | complete) "the sandbox should become active before sandbox control connects"
tg --url $runner.url checkpoint unwatch runner.sandbox.state.inserted $state_watch
success (tg --url $runner.url checkpoint wait --timeout 30s runner.process.start $start_watch 0 | complete) "the assigned process should start before sandbox control connects"
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.connect $connect_watch
success (job recv --tag $build --timeout 30sec) "the build should complete after sandbox control connects"
