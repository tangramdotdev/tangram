use ../../util/test.nu *

# A runner that restarts within its ttl and no longer has the directory of a sandbox that would stop destroys the sandbox, because the sandbox cannot start again without its files.

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

# Kill the runner, remove the sandbox's directory, and start the runner again.
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
rm --recursive --force ($runner.directory | path join 'sandboxes' $sandbox)
let runner = server start $runner

# The restarted runner's registration destroys the sandbox with the restart's error.
success (timeout 30s tg --url $remote.url --token $root_token sandbox status --until destroyed $sandbox | complete) 'the sandbox should be destroyed'
let data = tg --url $remote.url --token $root_token sandbox get $sandbox | from json | get data
assert equal $data.status destroyed
assert equal $data.error.message 'runner restarted'
