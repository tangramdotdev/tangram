use ../lib/test.nu *

# A remote runner pushes the output objects and waits for the push to complete before sending Finish.
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

# Hold the runner after the output push completes and before it sends Finish.
let pushed_watch = tg --url $runner.url checkpoint watch runner.process.output.push.finished | from json | get watch
let sent_watch = tg --url $runner.url checkpoint watch runner.process.control.finish.sent | from json | get watch
let received_watch = tg --url $remote.url --token $root_token checkpoint watch process.control.finish | from json | get watch
let expected = tg --url $local.url put 'tg.file("hello")' | str trim
let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nprintf hello > \"$TANGRAM_OUTPUT\""), "executable": true })'
let file = tg --url $local.url put $artifact | str trim
let build = job spawn {
	let job_id = job id
	let output = tg --url $local.url build --remote $file | complete
	$output | job send --tag $job_id 0
}
let pushed = timeout 30s tg --url $runner.url checkpoint wait runner.process.output.push.finished $pushed_watch 0 | complete
success $pushed "the runner should push the output"

# The output must be on the remote before the runner sends Finish.
success (tg --url $remote.url --token $root_token get $expected | complete) "the output should be on the remote before Finish is sent"
failure (timeout 1s tg --url $runner.url checkpoint wait runner.process.control.finish.sent $sent_watch 0 | complete) "Finish must not be sent before the output push completes"
failure (timeout 1s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $received_watch 0 | complete) "the remote must not receive Finish before the output push completes"

# Release the push and verify that Finish follows.
tg --url $runner.url checkpoint unwatch runner.process.output.push.finished $pushed_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.finish.sent $sent_watch 0 | complete) "the runner should send Finish after the push"
tg --url $runner.url checkpoint unwatch runner.process.control.finish.sent $sent_watch
success (timeout 30s tg --url $remote.url --token $root_token checkpoint wait process.control.finish $received_watch 0 | complete) "the remote should receive Finish after the push"
tg --url $remote.url --token $root_token checkpoint unwatch process.control.finish $received_watch
let output = job recv --tag $build --timeout 30sec
success $output "the build should complete after Finish is released"
let output = $output.stdout | str trim
assert equal ($output | split row '?' | first) $expected
assert equal (tg --url $local.url read $output) hello
