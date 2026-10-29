use ../../lib/test.nu *

# Reconnecting to the server without restarting the runner preserves its running processes.
let root_token = random chars
let remote = server spawn --name remote --preserve-keys --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: { cpus: 1, overcommit: { cpu: 1 }, id: $created.data.id, remote: "default", token: $created.token.token },
}
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			return 42;
		}
	',
}

# Hold the process on the runner after it exits and before it finishes.
let exit_watch = tg --url $runner.url checkpoint watch runner.process.exit | from json | get watch
let id = tg --url $local.url build --no-tokens --remote --detach $path | referent node
assert equal (tg --url $remote.url --token $root_token get $id | from json | get status) "started"
success (tg --url $runner.url checkpoint wait --timeout 30s runner.process.exit $exit_watch 0 | complete) "the process must run on the runner"

# Restart the remote while the runner holds the process.
let remote_pid = open ($remote.directory | path join 'lock') | into int
kill --signal 9 $remote_pid
wait_for_exit $remote_pid "the remote server must exit"
let remote = server start $remote

# Let the process finish after the restart.
tg --url $runner.url checkpoint continue runner.process.exit $exit_watch 0
tg --url $runner.url checkpoint unwatch runner.process.exit $exit_watch

let output = timeout 60s tg --url $local.url process wait $id | complete
success $output "the process must finish after the runner reconnects"
let data = tg --url $remote.url --token $root_token get $id | from json
assert equal $data.status "finished"
let error = $data | get -o error
let error = if ($error | describe) == "string" { tg --url $remote.url --token $root_token get $error } else { $error | to json --raw }
assert equal $data.exit 0 $"the process must survive the control reconnect: ($error)"
