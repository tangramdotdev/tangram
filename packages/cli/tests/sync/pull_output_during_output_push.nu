use ../../test.nu *

# A client pulls a process's output, conferred through the referent the process's wait returns, while the runner is still pushing that output. The pull waits for the push instead of failing.

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
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

# The build outputs a file so that the runner pushes its output.
let path = artifact {
	tangram.ts: '
		export default () => tg.file("hello");
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

# The process finishes before its push, and its wait names the output with the sync token.
let output = timeout 30s tg --url $alice_local.url wait $process | from json
let file = $output.output.value
assert ($file =~ "sync") "the output referent should carry the sync token"

# Alice grants Bob the process's output, and Bob pulls the conferred referent while the push is held.
tg --url $remote.url --token $alice.token grant $bob.user.id process_node_output $process
let pull = job spawn {
	let job_id = job id
	let output = tg --url $bob_local.url pull $file | complete
	$output | job send --tag $job_id 0
}
let output = try { job recv --tag $pull --timeout 5sec } catch { null }
if $output != null {
	error make { msg: $"the pull should wait while the push is held: ($output)" }
}

# Release the push. The pull completes and Bob reads the output.
tg --url $runner.url checkpoint continue runner.process.output.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.output.push.started $push_watch
success (job recv --tag $pull --timeout 30sec) "bob's pull should complete"
let output = tg --url $bob_local.url read ($file | split row '?' | first) | complete
success $output "bob should read the output"
snapshot ($output.stdout | str trim) 'hello'
