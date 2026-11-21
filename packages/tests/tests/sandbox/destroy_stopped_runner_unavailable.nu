use ../../util/test.nu *

# A destroy of a stopped sandbox whose runner is declared dead finishes, because the sandbox's scheduler records the destruction, and the runner removes the sandbox's directory once it connects again.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { runner_ttl: 3 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
tg --url $remote.url --token $root_token sandbox stop $sandbox

# Kill the runner and advance the remote's clock past the runner ttl so that the scheduler expires it.
let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
wait_for_exit $runner_pid
advance_time $remote 4sec

let output = timeout 30s tg --url $remote.url --token $root_token sandbox destroy --wait $sandbox | complete
success $output "the destroy should finish while the runner is declared dead"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed

# Start the runner again. It connects, and the destroyed sandbox stays destroyed.
let runner = server start $runner
success (timeout 30s tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | complete) "the runner should take a sandbox after it connects again"
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) destroyed
