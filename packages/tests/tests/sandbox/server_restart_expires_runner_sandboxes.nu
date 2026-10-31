use ../../util/test.nu *

# A single server that restarts after a runner died stops the runner's running sandboxes, because the restart loses the runner's registration, so its heartbeat can no longer expire.

let root_token = random chars
let remote = server spawn --name remote --config {
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let sandbox = tg --url $remote.url --token $root_token sandbox create --no-tokens | referent node
assert equal (tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data.status) started

# Kill the runner and then the remote, and start the remote again without the runner.
let runner_pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $runner_pid
wait_for_exit $runner_pid
kill --signal 9 $remote.pid
wait_for_exit $remote.pid
let remote = server start $remote
tg --url $remote.url --token $root_token index

let data = tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status stopped "the restarted server should stop the dead runner's sandbox"
assert equal $data.error.message "server restarted"
