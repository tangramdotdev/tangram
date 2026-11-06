use ../../util/test.nu *

# A remote runner that restarts on a fresh disk reconnects with the same ID, because the server that it connects to allocates its registration.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token } },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let config = {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: {
		id: $created.data.id,
		remote: default,
		token: $created.token.token,
	},
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let runner = server spawn --name runner --config $config
tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node | ignore

# Restart the runner, so that its registration advances.
server stop $runner
let runner = server start $runner
tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | referent node | ignore

# Crash the runner, and replace it with one that has the same ID and a fresh directory.
let pid = open ($runner.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let runner = server spawn --name fresh --config $config

# The new runner accepts work.
let output = tg --url $remote.url --token $root_token sandbox create --wait --no-tokens | complete
success $output 'the fresh runner should accept a sandbox'
