use ../lib/test.nu *

# A scheduled process starts in a pooled sandbox before sandbox control connects.

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
	runner: { id: $created.data.id, remote: default, sandbox_pool_size: 1, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
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
let claimed = timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.pool.take $pool_watch 0 | from json
tg --url $runner.url checkpoint unwatch runner.sandbox.pool.take $pool_watch
let connected = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.connect $connect_watch 0 | from json
let state = timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.state.inserted $state_watch 0 | from json
assert equal $state.params.sandbox $connected.params.sandbox "the sandbox should already have its scheduler-assigned ID"
assert equal $state.params.index $claimed.params.index "the scheduled sandbox should use the pooled physical sandbox"
tg --url $runner.url checkpoint unwatch runner.sandbox.state.inserted $state_watch
let started = timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | from json
assert ($started.params.process | str starts-with 'pcs_') "the assigned process should start before sandbox control connects"
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.connect $connect_watch
success (job recv --tag $build --timeout 30sec) "the build should complete after sandbox control connects"
