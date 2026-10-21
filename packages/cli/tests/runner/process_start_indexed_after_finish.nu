use ../lib/test.nu *

# A process's Start index batch that lands after its Finish batch is rejected with "cannot replace an existing process" and discarded, which drops the sandbox record the outcome sync needs.
let root_token = random chars
let remote = server spawn --preserve-keys --name remote --config {
	advanced: { checkpoints: true },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Hold the Start batch on the remote until the Finish batch has been committed, and hold the outcome sync on the runner until the Start batch has been discarded.
let start_params = { started_process: true } | to json --raw
let finish_params = { finished_process: true } | to json --raw
let start_watch = tg --url $remote.url --token $root_token checkpoint watch index.batch --params $start_params | from json | get watch
let finish_watch = tg --url $remote.url --token $root_token checkpoint watch index.batch --params $finish_params | from json | get watch
let start_finished_watch = tg --url $remote.url --token $root_token checkpoint watch index.batch.finished --params $start_params | from json | get watch
let received_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.finish | from json | get watch
let sync_watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.started | from json | get watch

let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\""), "executable": true })'
let file = tg --url $local.url put --no-tokens $artifact | referent node
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $file | complete
	$output | job send --tag $job_id 0
}

success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait index.batch $start_watch 0 | complete) "the remote should submit the Start batch"
let received = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $received_watch 0 | complete
success $received "the remote should receive Finish while the Start batch is held"
let process = $received.stdout | from json | get params.id
tg --url $remote.url --token $root_token checkpoint continue process.control.finish $received_watch 0
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait index.batch $finish_watch 0 | complete) "the remote should submit the Finish batch"
tg --url $remote.url --token $root_token checkpoint continue index.batch $finish_watch 0
let finished = timeout 30s tg --url $remote.url --token $root_token wait --source=index $process | from json
assert equal $finished.exit 0

# Release the Start batch now that the process is finished in the index.
tg --url $remote.url --token $root_token checkpoint continue index.batch $start_watch 0
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait index.batch.finished $start_finished_watch 0 | complete) "the Start batch should be processed after the Finish batch"
tg --url $remote.url --token $root_token checkpoint continue index.batch.finished $start_finished_watch 0

# Release the outcome sync, which authenticates as the process and resolves its sandbox through the index.
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.started $sync_watch

let output = job recv --tag $build --timeout 30sec
success $output "the build should return its outcome"
let file = $finished.output.value
let read = job spawn {
	let job_id = job id
	let output = tg --url $local.url read $file | complete
	$output | job send --tag $job_id 0
}
let output = try { job recv --tag $read --timeout 30sec } catch { null }
if $output != null {
	success $output "the output must be readable after the outcome sync"
	assert equal $output.stdout hello
} else {
	error make { msg: "the output read did not complete after the outcome sync" }
}

server stop $remote
let log = open --raw $remote.log
assert (not ($log | str contains "cannot replace an existing process")) "the Start batch must not be rejected"
