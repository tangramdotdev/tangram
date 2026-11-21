use ../../util/test.nu *

# A destroy of a stopped sandbox whose runner is down but not declared dead finishes once the runner registers again, because the sandbox's scheduler removes the directory through the runner and records the destruction.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let sandbox = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node
tg --url $remote.url --token $root_token sandbox stop --wait $sandbox

# Kill the runner, and destroy the stopped sandbox while the scheduler still has the runner.
let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
wait_for_exit $runner_pid
success (tg --url $remote.url --token $root_token sandbox destroy $sandbox | complete) "the destroy should be recorded"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) destroying

# Start the runner again. The sandbox is destroyed once the scheduler removes its directory through the runner.
let runner = server start $runner
success (timeout 30s tg --url $remote.url --token $root_token sandbox destroy --wait $sandbox | complete) "the destroy should finish once the runner registers again"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed
