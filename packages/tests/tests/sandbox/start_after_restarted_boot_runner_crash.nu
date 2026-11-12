use ../../util/test.nu *

# A sandbox that was stopped and started, and that was running when its runner was killed, starts again after the runner restarts.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { checkpoints: true, multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}
let url = $remote.url

# Stop and start a sandbox.
let sandbox = tg --url $url --token $root_token sandbox create --wait --no-tokens | referent node
tg --url $url --token $root_token sandbox stop --wait $sandbox
tg --url $url --token $root_token sandbox start --wait $sandbox

# Kill the runner, then start it again and wait for it to stop the sandbox.
let stopped = checkpoint watch runner.control.stop_sandbox --params { sandbox: $sandbox } --token $root_token --url $url
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let runner = server start $runner
checkpoint pass $stopped
checkpoint unwatch $stopped
tg --url $url --token $root_token sandbox stop --wait $sandbox

# The sandbox starts again.
let output = timeout 30s tg --url $url --token $root_token sandbox start --wait $sandbox | complete
success $output "the sandbox should start"
assert equal (tg --url $url --token $root_token sandbox get $sandbox | from json | get data.status) started "the sandbox should be started"
