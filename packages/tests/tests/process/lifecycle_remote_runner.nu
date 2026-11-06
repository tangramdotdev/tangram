use ../../util/test.nu *
use ../../util/lifecycle.nu *

# Every process method can be called at every stage of a process's lifecycle when its runner is on another server.

let root_token = random chars
let remote = server spawn --name remote --config {
	advanced: { multi_server: true },
	authentication: { root: { token: $root_token } },
	indexer: { id: 'idx_0000000000000000000000000000' },
	roles: [api indexer scheduler],
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}
let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	runner: {
		cpus: 1,
		id: $created.data.id,
		overcommit: { cpu: 1 },
		remote: default,
		token: $created.token.token,
	},
	scheduler: { default_cpu: 1, default_memory: 1_073_741_824 },
}

# Build the commands on a client server, which pushes them to the remote.
let local = server spawn --name local --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
}
let commands = lifecycle_commands
tg push $commands.hello
tg push $commands.spin

$env.TANGRAM_URL = $remote.url
$env.TANGRAM_TOKEN = $root_token

process_lifecycle $commands
