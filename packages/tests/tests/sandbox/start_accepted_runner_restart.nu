use ../../util/test.nu *

# A start of a stopped sandbox whose boot the runner accepted and that has not connected when the runner crashes fails with the restart's error, and the sandbox stops, when the runner restarts.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 300 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

# Create a sandbox, and stop it.
let sandbox = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node
tg --url $remote.url --token $root_token sandbox stop --wait $sandbox

# Start the sandbox, and hold the connection of its next boot, which the runner accepted.
let connect = checkpoint watch sandbox.control.connect --params { sandbox: $sandbox } --token $root_token --url $remote.url
let url = $remote.url
let start = job spawn {
	let id = job id
	timeout 60s tg --url $url --token $root_token sandbox start --wait $sandbox | complete | job send --tag $id 0
}
checkpoint wait $connect | ignore

# Crash the runner, start it again, and wait for the scheduler to finish its connection.
let added = checkpoint watch scheduler.runner.added --params { runner: $created.data.id } --token $root_token --url $remote.url
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let runner = server start $runner
checkpoint wait $added | ignore
checkpoint unwatch $added

# The connection stops the sandbox with the restart's error, and the start fails.
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until stopped $sandbox | complete) "the sandbox should stop"
let data = tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.boot 1 "the accepted boot should end"
assert equal $data.error.message 'runner restarted'
let output = job recv --tag $start --timeout 60sec
failure $output "the start should fail"
assert ($output.stderr | str contains 'runner restarted') "the start should report the restart"
checkpoint unwatch $connect
