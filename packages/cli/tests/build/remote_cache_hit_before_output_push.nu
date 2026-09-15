use ../../test.nu *

# A finished remote process is a cache hit while the runner is still pushing its output. Readers of the output wait for the push to complete instead of failing.

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --cloud --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with checkpoints enabled.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

# Create user credentials and spawn two local servers so that the second build cannot hit the first local server's cache.
let alice = tg --url $remote.url login --verbose --name alice | from json
let first = server spawn --name first --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let second = server spawn --name second --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
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

# Start the first build and wait for it to reach its output push.
let process = tg --url $first.url build --detach --remote --user $alice.user.id $path | str trim
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.output.push.started $push_watch 0 | complete
success $output "the first build should reach its output push"

# The second build must be a cache hit for the first process while its output push is held.
let cached = tg --url $second.url build --detach --remote --user $alice.user.id $path | str trim
assert equal $cached $process "the second build should reuse the first process while its output push is held"

# The cached process's wait completes and names the output file.
let output = timeout 30s tg --url $second.url wait $cached | from json
let file = $output.output.value

# Reading the output blocks while the push is held.
let read = job spawn {
	let job_id = job id
	let output = tg --url $second.url read $file | complete
	$output | job send --tag $job_id 0
}
let output = try { job recv --tag $read --timeout 3sec } catch { null }
if $output != null {
	error make { msg: $"the output read should wait while the push is held: ($output)" }
}

# Release the push. The read carries the output's sync token, so it retries until the contents land.
tg --url $runner.url checkpoint continue runner.process.output.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.output.push.started $push_watch

# The read completes with the pushed contents.
let output = job recv --tag $read --timeout 30sec
success $output "the output read should complete after the push finishes"
snapshot ($output.stdout | str trim) 'hello'
