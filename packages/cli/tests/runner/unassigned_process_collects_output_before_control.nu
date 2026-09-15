use ../../test.nu *

# A child with a checked-in output finishes before its ID arrives, while indexing waits for the ID.

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
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}

# Create user credentials and spawn the local server.
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name alice-local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let params = { process: 'None' } | to json --raw
let control_watch = tg --url $runner.url checkpoint watch runner.process.control.connect --params $params | from json | get watch
let stored_watch = tg --url $runner.url checkpoint watch runner.process.output.stored --params $params | from json | get watch
let finished_watch = tg --url $runner.url checkpoint watch runner.process.finished --params $params | from json | get watch
let sent_watch = tg --url $runner.url checkpoint watch runner.process.control.finish.sent --params $params | from json | get watch
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

success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.connect $control_watch 0 | complete) "the unassigned child should reach control"
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.output.stored $stored_watch 0 | complete) "the child should store its output without a process ID"
tg --url $runner.url checkpoint unwatch runner.process.output.stored $stored_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.finished $finished_watch 0 | complete) "the child should finish without its process ID"
tg --url $runner.url checkpoint unwatch runner.process.finished $finished_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.finish.sent $sent_watch 0 | complete) "the child should send Finish without its process ID"
tg --url $runner.url checkpoint unwatch runner.process.control.finish.sent $sent_watch
let index = job spawn {
	let job_id = job id
	let output = tg --url $runner.url index | complete
	$output | job send --tag $job_id 0
}
assert equal (try { job recv --tag $index --timeout 1sec } catch { null }) null "indexing should wait for the process ID"
tg --url $runner.url checkpoint unwatch runner.process.control.connect $control_watch
success (job recv --tag $index --timeout 30sec) "indexing should finish after the ID arrives"

let output = job recv --tag $build --timeout 30sec
success $output "the unassigned child should complete after receiving its ID"
let file = $output.stdout | str trim
let read = tg --url $local.url read $file | complete
success $read
assert equal $read.stdout hello
