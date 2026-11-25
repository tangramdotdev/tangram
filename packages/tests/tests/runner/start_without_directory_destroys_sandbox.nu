use ../../util/test.nu *

# A sandbox whose runner restarts and no longer has the sandbox's directory stays stopped, and a start of it fails and destroys it, because the sandbox cannot start again without its files.

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

# Create a sandbox, which stops rather than being destroyed when its runner is lost.
let sandbox = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node

# Kill the runner, remove the sandboxes' directories, and start the runner again.
let pid = server_pid $runner.directory
kill --signal 9 $pid
wait_for_exit $pid
^chmod -R u+rwx ($runner.directory | path join 'sandboxes')
rm --recursive --force ($runner.directory | path join 'sandboxes')
let runner = server start $runner

# The restarted runner's first connection stops the sandbox with the restart's error.
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until stopped $sandbox | complete) 'the sandbox should stop'
let data = tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.error.message 'runner restarted'

# A start of the sandbox fails, and destroys the sandbox with the error of its lost directory.
tg --url $remote.url --token $root_token sandbox start --wait $sandbox | complete | ignore
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until destroyed $sandbox | complete) 'the sandbox should be destroyed'
let data = tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status destroyed
assert ($data.error.message | str contains 'directory was lost') 'the error should name the lost directory'
