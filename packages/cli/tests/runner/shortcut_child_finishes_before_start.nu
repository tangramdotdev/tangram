use ../../test.nu *

# A shortcut child that finishes before the runner has started it on the remote must still deliver its result to the guest client.

if $nu.os-info.name != 'linux' {
	skip_test 'this test requires linux'
}

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with the control pools, as deployed.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: {
		id: $created.data.id,
		process_control_pool_size: 8,
		remote: 'default',
		sandbox_control_pool_size: 8,
		sandbox_pool_size: 8,
		token: $created.token.token,
	},
}

let dir = tg --url $remote.url --token $root_token put -k directory 'tg.directory({})' | str trim

# Hold the child's command push so the remote does not learn about the child, and hold the spawn reply so the child finishes before the guest client can wait for it.
let push_watch = (
	tg --url $runner.url checkpoint watch runner.process.command.push.started
	| from json
	| get watch
)
let spawn_watch = (
	tg --url $runner.url checkpoint watch process.spawn.child.add
	| from json
	| get watch
)
let finished_watch = (
	tg --url $runner.url checkpoint watch runner.process.finished
	| from json
	| get watch
)

# Run a sandboxed parent that spawns a trivial child through the guest URL.
let run = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $root_token run --sandbox --executable /bin/sh $dir -- -c '/opt/tangram/bin/tangram run --sandbox --executable /bin/sh "$0" -- -c "echo hello-from-child"' $dir | complete
	$output | job send --tag $job_id 0
}

# The child's push and spawn reply are held while the child runs to completion.
let output = timeout 60s tg --url $runner.url checkpoint wait runner.process.command.push.started $push_watch 0 | complete
success $output "the child should reach its command push"
let output = timeout 60s tg --url $runner.url checkpoint wait process.spawn.child.add $spawn_watch 0 | complete
success $output "the child spawn should reach its reply"
let output = timeout 60s tg --url $runner.url checkpoint wait runner.process.finished $finished_watch 0 | complete
success $output "the child should finish while its push and spawn reply are held"
tg --url $runner.url checkpoint continue runner.process.finished $finished_watch 0

# Release the spawn reply: the guest client now waits for a child that finished before the remote knows it.
tg --url $runner.url checkpoint continue process.spawn.child.add $spawn_watch 0
tg --url $runner.url checkpoint unwatch process.spawn.child.add $spawn_watch

# The parent finishes only after the child's result reached the guest client.
let output = timeout 60s tg --url $runner.url checkpoint wait runner.process.finished $finished_watch 1 | complete
success $output "the parent should finish"
tg --url $runner.url checkpoint continue runner.process.finished $finished_watch 1
tg --url $runner.url checkpoint unwatch runner.process.finished $finished_watch

# Release the push.
tg --url $runner.url checkpoint continue runner.process.command.push.started $push_watch 0
tg --url $runner.url checkpoint unwatch runner.process.command.push.started $push_watch

let output = try { job recv --tag $run --timeout 60sec } catch { null }
if $output == null {
	error make { msg: "the run did not complete" }
}
success $output "the child spawned from inside the sandbox should succeed when it finishes before the remote starts it"
assert ($output.stdout | str contains "hello-from-child") "the child output should reach the client"
