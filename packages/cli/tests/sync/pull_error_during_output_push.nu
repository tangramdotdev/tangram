use ../../test.nu *

# A process error carries the receiving sync token while its object is still being pushed.

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
	sync: { control: { index_timeout: 60 } },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

# Alice builds and Bob pulls.
let alice = tg --url $remote.url login --verbose --name alice | from json
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let bob = tg --url $remote.url login --verbose --name bob | from json
let bob_local = server spawn --name bob-local --config {
	remotes: { default: { token: $bob.token, url: $remote.url } },
}

# The build fails with an error object that the runner must push.
let path = artifact {
	tangram.ts: '
		export default () => { throw new Error("delayed error"); };
	'
}

# Hold the output push.
let push_watch = (
	tg --url $runner.url checkpoint watch runner.process.output.push.started
	| from json
	| get watch
)

# Start the build and wait for it to reach its output push.
let process = tg --url $alice_local.url build --detach --remote --user $alice.user.id $path | str trim
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.output.push.started $push_watch 0 | complete
success $output "the build should reach its output push"

# The process finishes before its push, and its wait names the error with the sync token.
let output = timeout 30s tg --url $alice_local.url wait $process | from json
let error = $output.error
assert ($error =~ "sync") "the error referent should carry the sync token"

# Alice grants Bob the process's error, and Bob pulls it while the push is held.
tg --url $remote.url --token $alice.token grant $bob.user.id process_node_error $process
let pull = job spawn {
	let job_id = job id
	let output = tg --url $bob_local.url pull $error | complete
	$output | job send --tag $job_id 0
}
let output = try { job recv --tag $pull --timeout 5sec } catch { null }
if $output != null {
	error make { msg: $"the pull should wait while the push is held: ($output)" }
}

# Release the push. The pull completes and Bob reads the error.
tg --url $runner.url checkpoint continue runner.process.output.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.output.push.started $push_watch
success (job recv --tag $pull --timeout 30sec) "bob's pull should complete"
let output = tg --url $bob_local.url get ($error | split row '?' | first) | complete
success $output "bob should read the error"
assert ($output.stdout | str contains 'delayed error')
