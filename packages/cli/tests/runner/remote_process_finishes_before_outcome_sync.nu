use ../lib/test.nu *

# A remote runner sends Finish concurrently with the outcome sync by default.
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

# Hold the outcome sync while the remote receives Finish.
let sync_watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.started | from json | get watch
let synced_watch = tg --url $runner.url checkpoint watch runner.process.outcome.sync.finished | from json | get watch
let finished_watch = tg --url $runner.url checkpoint watch runner.process.control.finish.succeeded | from json | get watch
let received_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.finish | from json | get watch
let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\""), "executable": true })'
let file = tg --url $local.url put --no-tokens $artifact | referent node
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $file | complete
	$output | job send --tag $job_id 0
}
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.outcome.sync.started $sync_watch 0 | complete) "the runner should reach the outcome sync"
let received = timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $received_watch 0 | complete
success $received "the remote should receive Finish while the outcome sync is blocked"
let process = $received.stdout | from json | get params.id

# Let the server process Finish while outcome sync remains paused.
tg --url $remote.url --token $root_token checkpoint unwatch process.control.finish $received_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.finish.succeeded $finished_watch 0 | complete) "Finish must succeed while outcome sync is paused"
tg --url $runner.url checkpoint unwatch runner.process.control.finish.succeeded $finished_watch
let output = job recv --tag $build --timeout 30sec
success $output "the build must return its outcome before sync completes"
let outcome = timeout 30s tg --url $remote.url --token $alice.token wait --source=index $process | from json
assert equal $outcome.exit 0
let file = $outcome.output.value
assert equal ($output.stdout | str trim | split row '?' | first) ($file | split row '?' | first)
let params = $'http://localhost/($file)' | url parse | get params
assert ($params | where {|param| $param.key starts-with 'tokens[' } | any {|param|
	let body = $param.value | split row '.' | get 1 | decode base64 | decode utf-8 | from json
	($body.resource | str starts-with 'syn_') and ('sync_read' in $body.permissions)
}) "the outcome must carry sync authorization"

# Reading the outcome must wait for its objects rather than fail authorization.
let read = job spawn {
	let job_id = job id
	let output = tg --url $local.url read $file | complete
	$output | job send --tag $job_id 0
}
let output = try { job recv --tag $read --timeout 1sec } catch { null }
assert ($output == null) "the output must remain unavailable while sync is paused"

# Release sync and require the previously returned referent to become readable.
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.started $sync_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.outcome.sync.finished $synced_watch 0 | complete) "the outcome sync should complete"
tg --url $runner.url checkpoint unwatch runner.process.outcome.sync.finished $synced_watch
let output = job recv --tag $read --timeout 30sec
success $output "the output read must complete after sync"
assert equal $output.stdout hello
