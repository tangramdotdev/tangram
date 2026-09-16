use ../../test.nu *

# A scheduled process accepts local stdio before process control connects.
let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: default, token: $created.token.token },
}
let control_watch = tg --url $runner.url checkpoint watch runner.process.control.connect | from json | get watch
let finished_watch = tg --url $runner.url checkpoint watch runner.process.finished | from json | get watch
let state_watch = tg --url $runner.url checkpoint watch runner.process.state.inserted | from json | get watch
let artifact = 'tg.file({ "contents": tg.blob("#!/bin/sh\nread line\ntest \"$line\" = ready"), "executable": true })'
let file = tg --url $remote.url --token $root_token put $artifact | str trim
let spawn = job spawn {
	let job_id = job id
	let output = tg --url $remote.url --token $root_token run --sandbox --stdin pipe --stdout null --stderr null $file | complete
	$output | job send --tag $job_id 0
}
let state = timeout 10s tg --url $runner.url checkpoint wait runner.process.state.inserted $state_watch 0 | complete
if $state.exit_code != 0 {
	let output = job recv --tag $spawn --timeout 1sec
	success $output
	success $state
}
let process = $state.stdout | from json | get params.process
tg --url $runner.url checkpoint unwatch runner.process.state.inserted $state_watch
success (timeout 30s tg --url $runner.url checkpoint wait runner.process.control.connect $control_watch 0 | complete)

# Write stdin directly on the runner while the owner connection is blocked.
success ("ready\n" | timeout 10s tg --url $runner.url process stdio write $process --stream stdin | complete)
success (timeout 10s tg --url $runner.url checkpoint wait runner.process.finished $finished_watch 0 | complete) "local stdio must not wait for the control connection"
tg --url $runner.url checkpoint unwatch runner.process.finished $finished_watch
tg --url $runner.url checkpoint unwatch runner.process.control.connect $control_watch
let output = job recv --tag $spawn --timeout 30sec
success $output
let output = timeout 30s tg --url $remote.url --token $root_token wait $process | from json
assert equal $output.exit 0
