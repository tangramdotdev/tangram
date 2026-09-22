use ../lib/test.nu *

# A shortcut child waits for its ID, then runs and checks in its output before runner indexing completes.

let root_token = random chars

# Spawn the remote and create the runner.
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json

# Spawn the runner with checkpoints enabled.
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, process_control_connection_pool_size: 0, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let params = { process: 'None' } | to json --raw
let control_watch = tg --url $runner.url checkpoint watch runner.process.control.connect --params $params | from json | get watch
let start_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
let path = artifact {
	tangram.ts: '
		export default async function () {
			const file = await tg.file({ contents: "#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\"", executable: true });
			return tg.build(file);
		}
	',
}
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $path | complete
	$output | job send --tag $job_id 0
}

success (timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | complete) "the parent should start"
tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.connect $control_watch 0 | complete) "the unassigned child should reach control"
let response_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.output | from json | get watch
tg --url $runner.url checkpoint unwatch runner.process.control.connect $control_watch
let response = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.output $response_watch 0 | from json
let child = $response.params.process
assert ($child | str starts-with 'pcs_') "control should assign a process ID"
let started = timeout 1s tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | complete
assert equal $started.exit_code 124 "the child should not start before receiving the control response"

let params = { process: $child } | to json --raw
let index_watch = tg --url $runner.url checkpoint watch runner.process.index.started --params $params | from json | get watch
let stored_watch = tg --url $runner.url checkpoint watch runner.process.output.stored --params $params | from json | get watch
let finished_watch = tg --url $runner.url checkpoint watch runner.process.finished --params $params | from json | get watch
let sent_watch = tg --url $runner.url checkpoint watch runner.process.control.finish.sent --params $params | from json | get watch
tg --url $remote.url --token $root_token checkpoint unwatch process.control.output $response_watch
let started = timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | from json
assert equal $started.params.process $child "the child should start with its assigned ID"
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.index.started $index_watch 0 | complete) "the child should reach initial indexing"
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.output.stored $stored_watch 0 | complete) "the child should store its output before initial indexing completes"
tg --url $runner.url checkpoint unwatch runner.process.output.stored $stored_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.finished $finished_watch 0 | complete) "the child should finish before initial indexing completes"
tg --url $runner.url checkpoint unwatch runner.process.finished $finished_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.finish.sent $sent_watch 0 | complete) "the child should send Finish before initial indexing completes"
tg --url $runner.url checkpoint unwatch runner.process.control.finish.sent $sent_watch
tg --url $runner.url checkpoint unwatch runner.process.index.started $index_watch
success (timeout 30s tg --url $runner.url index | complete) "indexing should finish after initial indexing is released"
let status = tg --url $runner.url --token $root_token process get $child | from json | get status
assert equal $status finished "the finished write should be indexed once initial indexing is released"

let output = job recv --tag $build --timeout 30sec
success $output "the shortcut child should complete with its own output grants"
let file = $output.stdout | str trim
let read = tg --url $local.url read $file | complete
success $read
assert equal $read.stdout hello
