use ../lib/test.nu *

# A log referent authorizes a pull while the runner is still transferring the finished blob.

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

# The build writes a log that the runner includes in its result push.
let path = artifact {
	tangram.ts: '
		export default () => { console.log("hello"); return tg.file("output"); };
	'
}

# Hold the output push.
let push_watch = (
	tg --url $runner.url checkpoint watch runner.process.output.push.started
	| from json
	| get watch
)

# Start the build and wait for it to reach its output push.
let process = tg --url $alice_local.url build --no-tokens --detach --remote --user $alice.user.id $path | referent node
let output = timeout 30s tg --url $runner.url checkpoint wait runner.process.output.push.started $push_watch 0 | complete
success $output "the build should reach its output push"

# Finish includes a log referent with the authorization token for the pending sync.
timeout 30s tg --url $alice_local.url wait $process | ignore
let output = tg --url $remote.url --token $alice.token get --source=index $process | from json
let log = $output.log
assert ($log =~ 'tokens\[') "the log referent should carry the authorization token for the sync"

# Bob obtains a log referent using node permission while the outcome push is held.
tg --url $remote.url --token $alice.token grant $bob.user.id process_node $process
let output = tg --url $bob_local.url get --remote --source=index $process | from json
let log = $output.log
assert ($log =~ 'tokens\[') "node permission should preserve the stored authorization token for the sync"
let pull = job spawn {
	let job_id = job id
	let output = tg --url $bob_local.url pull $log | complete
	$output | job send --tag $job_id 0
}
let output = try { job recv --tag $pull --timeout 5sec } catch { null }
if $output != null {
	error make { msg: $"the pull should wait while the push is held: ($output)" }
}

# Release the push. The pull completes and Bob reads the log.
tg --url $runner.url checkpoint continue runner.process.output.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.output.push.started $push_watch
success (job recv --tag $pull --timeout 30sec) "bob's pull should complete"
let output = tg --url $bob_local.url log $process --no-timeout | complete
success $output "bob should read the finished log"
snapshot ($output.stdout | str trim) 'hello'
