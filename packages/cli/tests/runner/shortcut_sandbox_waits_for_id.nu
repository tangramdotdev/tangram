use ../../test.nu *

# A shortcut can claim an IDless physical sandbox, but cannot activate it until control delivers its identity.

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

let params = { sandbox: 'None' } | to json --raw
let connect_watch = tg --url $runner.url checkpoint watch runner.sandbox.control.connect --params $params | from json | get watch
let pool_watch = tg --url $runner.url checkpoint watch runner.sandbox.pool.take | from json | get watch
let state_watch = tg --url $runner.url checkpoint watch runner.sandbox.state.inserted | from json | get watch
let start_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
let path = artifact {
	tangram.ts: '
		export default async function () {
			const file = await tg.file({ contents: "#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\"", executable: true });
			return tg.build(file).sandbox();
		}
	',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

# Let the scheduled parent claim its sandbox and start.
success (timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.pool.take $pool_watch 0 | complete) "the parent should claim a pooled sandbox"
tg --url $runner.url checkpoint continue runner.sandbox.pool.take $pool_watch 0
success (timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.state.inserted $state_watch 0 | complete) "the parent sandbox should become active"
tg --url $runner.url checkpoint continue runner.sandbox.state.inserted $state_watch 0
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | complete) "the parent should start"
tg --url $runner.url checkpoint continue runner.process.start $start_watch 0

# The child claims an IDless physical sandbox while its control connection is still pending.
success (timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.control.connect $connect_watch 0 | complete) "the shortcut sandbox should reach control without an ID"
let claimed = timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.pool.take $pool_watch 1 | from json
tg --url $runner.url checkpoint unwatch runner.sandbox.pool.take $pool_watch
let response_watch = tg --url $remote.url --token $root_token checkpoint watch sandbox.control.connect | from json | get watch
tg --url $runner.url checkpoint unwatch runner.sandbox.control.connect $connect_watch
let response = timeout 30s tg --url $remote.url --token $root_token checkpoint wait sandbox.control.connect $response_watch 0 | from json
let sandbox = $response.params.sandbox
assert ($sandbox | str starts-with 'sbx_') "control should assign a sandbox ID"
let state = timeout 1s tg --url $runner.url checkpoint wait runner.sandbox.state.inserted $state_watch 1 | complete
assert equal $state.exit_code 124 "the shortcut sandbox should not become active before receiving its identity"
let started = timeout 1s tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | complete
assert equal $started.exit_code 124 "the child should not start before its sandbox receives its identity"

# Receiving the identity activates the same physical sandbox and allows the child to run.
tg --url $remote.url --token $root_token checkpoint unwatch sandbox.control.connect $response_watch
let state = timeout 30s tg --url $runner.url checkpoint wait runner.sandbox.state.inserted $state_watch 1 | from json
assert equal $state.params.sandbox $sandbox "the shortcut sandbox should become active with its assigned ID"
assert equal $state.params.index $claimed.params.index "the shortcut should use the physical sandbox claimed before control connected"
tg --url $runner.url checkpoint unwatch runner.sandbox.state.inserted $state_watch
let started = timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | from json
assert ($started.params.process | str starts-with 'pcs_') "the child should start with its assigned process ID"
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch
let output = job recv --tag $build --timeout 30sec
success $output "the child should complete with its own sandbox and output grants"
let read = tg --url $local.url read ($output.stdout | str trim) | complete
success $read
assert equal $read.stdout hello
