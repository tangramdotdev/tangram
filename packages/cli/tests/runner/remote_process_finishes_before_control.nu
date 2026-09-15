use ../../test.nu *

# A remote runner finishes a checked-in output before control or initial indexing completes.

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

for checkpoint in [runner.process.control.connect process.control.output process.control.index.started runner.process.index.started] {
	let receiver = if ($checkpoint | str starts-with 'process.') { $remote } else { $runner }
	let receiver_token = if ($checkpoint | str starts-with 'process.') { $root_token } else { '' }
	let control_watch = tg --url $receiver.url --token $receiver_token checkpoint watch $checkpoint | from json | get watch
	let stored_watch = tg --url $runner.url checkpoint watch runner.process.output.stored | from json | get watch
	let finished_watch = tg --url $runner.url checkpoint watch runner.process.finished | from json | get watch
	let sent_watch = tg --url $runner.url checkpoint watch runner.process.control.finish.sent | from json | get watch
	let received_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.finish | from json | get watch

	let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf \"%s\" \"$1\" > \"$TANGRAM_OUTPUT\""), "executable": true })'
	let file = tg --url $local.url put $artifact | str trim
	let build = job spawn {
		let job_id = job id
		let output = tg --url $local.url build --remote $file --arg-string $checkpoint | complete
		$output | job send --tag $job_id 0
	}

	# Store the output while the connection or initial indexing is held.
	success (timeout 30s tg --url $receiver.url --token $receiver_token checkpoint wait $checkpoint $control_watch 0 | complete) "should reach $checkpoint"
	success (timeout 30s tg --url $runner.url checkpoint wait runner.process.output.stored $stored_watch 0 | complete) "output collection should not wait for control"
	tg --url $runner.url checkpoint unwatch runner.process.output.stored $stored_watch
	success (timeout 30s tg --url $runner.url checkpoint wait runner.process.finished $finished_watch 0 | complete) "completion should not wait for the control connection or indexing"
	tg --url $runner.url checkpoint unwatch runner.process.finished $finished_watch
	success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.finish.sent $sent_watch 0 | complete) "Finish should be queued before control returns"
	tg --url $runner.url checkpoint unwatch runner.process.control.finish.sent $sent_watch
	tg --url $receiver.url --token $receiver_token checkpoint unwatch $checkpoint $control_watch

	success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $received_watch 0 | complete) "the runner should send Finish"
	tg --url $remote.url --token $root_token checkpoint unwatch process.control.finish $received_watch
	let output = job recv --tag $build --timeout 30sec
	success $output "the build should complete after control is released"
	let file = $output.stdout | str trim
	assert equal (tg --url $local.url read $file) $checkpoint
}
