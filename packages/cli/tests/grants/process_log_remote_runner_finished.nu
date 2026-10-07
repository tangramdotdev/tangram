use ../lib/test.nu *

# A finished process retains the sync token for its log, so a process node reader can read the log across servers.

let root_token = random chars

# The remote authenticates users,  schedules work but holds no runner role, so the build can only complete by way of the separate runner.
let remote = server spawn --name remote --cloud --preserve-keys --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.data.id, remote: 'default', token: $created.token.token },
}

let alice = tg --url $remote.url login --verbose --name alice | from json
let eve = tg --url $remote.url login --verbose --name eve | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $alice.token, url: $remote.url } },
}

# Alice builds a process on the runner that writes to both stdout and stderr.
let path = artifact {
	tangram.ts: '
		export default function () {
			console.log("loghello");
			console.error("logerror");
			return 0;
		}
	'
}
let process = tg --url $local.url build --no-tokens --remote --detach $path | referent node
success (tg --url $remote.url --token $alice.token wait --source=index $process | complete)

# Wait for the process to finish with its log blob.
tg --url $remote.url --token $alice.token index

# The owner reads the finished log across servers. Each of the process's streams is written to the corresponding stream of the log command.
let owner = tg --url $local.url log --no-timeout $process | complete
success $owner "the owner must read the finished log of a process run by the runner."
snapshot --normalize $owner.stdout '
	loghello

'
assert ($owner.stderr | str contains 'logerror') "the owner must read the finished stderr."

# Eve reads the finished log using node permission and the stored sync token.
tg --url $remote.url --token $alice.token grant $eve.user.id process_node $process | ignore
let node_only = tg --url $remote.url --token $eve.token log $process | complete
success $node_only "node permission must allow reading the finished log through its sync token."
snapshot --normalize $node_only.stdout '
	loghello

'
assert ($node_only.stderr | str contains 'logerror') "node permission must allow reading the finished stderr through its sync token."
