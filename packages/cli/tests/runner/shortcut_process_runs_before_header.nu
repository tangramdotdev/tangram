use ../lib/test.nu *

# A shortcut child derives its own ID, so it starts, stores its output, and sends Finish while the remote holds its control header.

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
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

let start_watch = tg --url $runner.url checkpoint watch runner.process.start | from json | get watch
let header_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.header | from json | get watch
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

# Release the scheduled parent's start and control header.
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 0 | complete) "the parent should start"
tg --url $runner.url checkpoint continue runner.process.start $start_watch 0
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.header $header_watch 0 | complete) "the parent should receive its control header"
tg --url $remote.url --token $root_token checkpoint continue process.control.header $header_watch 0

# The child starts with an ID that the runner derived.
let started = timeout 30s tg --url $runner.url checkpoint wait runner.process.start $start_watch 1 | from json
let child = $started.params.process
assert ($child | str starts-with 'pcs_') "the child should start with a process ID"
let params = { process: $child } | to json --raw
let stored_watch = tg --url $runner.url checkpoint watch runner.process.output.stored --params $params | from json | get watch
let sent_watch = tg --url $runner.url checkpoint watch runner.process.control.finish.sent --params $params | from json | get watch
tg --url $runner.url checkpoint unwatch runner.process.start $start_watch

# The remote holds the child's control header while the child stores its output and sends Finish.
let header = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.header $header_watch 1 | from json
assert equal $header.params.process $child "the remote should hold the child's control header"
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.output.stored $stored_watch 0 | complete) "the child should store its output before its control header returns"
tg --url $runner.url checkpoint unwatch runner.process.output.stored $stored_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.finish.sent $sent_watch 0 | complete) "the child should send Finish before its control header returns"
tg --url $runner.url checkpoint unwatch runner.process.control.finish.sent $sent_watch
tg --url $remote.url --token $root_token checkpoint unwatch process.control.header $header_watch

# The build completes with the child's output.
let output = job recv --tag $build --timeout 30sec
success $output "the shortcut child should complete with its own output permissions"
let file = $output.stdout | str trim
let read = tg --url $local.url read $file | complete
success $read
assert equal $read.stdout hello
