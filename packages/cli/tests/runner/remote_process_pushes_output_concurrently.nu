use ../lib/test.nu *

# A remote runner can send Finish concurrently with the output push when awaiting pushes is disabled.
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
	process: { await_push: false },
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Hold the output push while the remote receives Finish.
let push_watch = tg --url $runner.url checkpoint watch runner.process.output.push.started | from json | get watch
let pushed_watch = tg --url $runner.url checkpoint watch runner.process.output.push.finished | from json | get watch
let received_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.finish | from json | get watch
let expected = tg --url $local.url put 'tg.file("hello")' | str trim
let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\""), "executable": true })'
let file = tg --url $local.url put $artifact | str trim
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $file | complete
	$output | job send --tag $job_id 0
}
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.output.push.started $push_watch 0 | complete) "the runner should reach the output push"
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $received_watch 0 | complete) "the remote should receive Finish while the output push is blocked"

# Release the push before letting the remote handle Finish.
tg --url $runner.url checkpoint unwatch runner.process.output.push.started $push_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.output.push.finished $pushed_watch 0 | complete) "the output push should complete while Finish is held"
tg --url $runner.url checkpoint unwatch runner.process.output.push.finished $pushed_watch
tg --url $remote.url --token $root_token checkpoint unwatch process.control.finish $received_watch
let output = job recv --tag $build --timeout 30sec
success $output "the build should complete with concurrent pushes"
let output = $output.stdout | str trim
assert equal ($output | split row '?' | first) $expected
assert equal (tg --url $local.url read $output) hello
