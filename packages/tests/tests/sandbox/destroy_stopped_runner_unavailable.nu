use ../../util/test.nu *

# Destroying a stopped sandbox fails while its runner is unavailable, and the sandbox remains stopped.

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

let output = tg --url $remote.url --token $root_token sandbox destroy $sandbox | complete
failure $output "destroying the sandbox should fail while its runner is unavailable"
assert ($output.stderr | str contains "the sandbox's runner is unavailable")
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) stopped
