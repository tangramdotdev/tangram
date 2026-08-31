use ../../test.nu *

# Once a process run by a remote runner has its log compacted, the log becomes a blob object on the remote, so reading it requires a grant on that object rather than just the process node. The log entries were written by the runner as the process principal and the blob was created by the remote's indexer, so the owner reaches it only through the process log link.

let root_token = random chars

# The remote authenticates users, compacts process logs, and schedules work but holds no runner role, so the build can only complete by way of the separate runner.
let remote = server spawn --name remote --cloud --preserve-keys --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.runner.id, remote: 'default', token: $created.token.token },
}

let alice = tg --url $remote.url login --verbose --name alice | from json
let eve = tg --url $remote.url login --verbose --name eve | from json
let local = server spawn --name alice-local --config {
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
let process = tg --url $local.url build --remote --detach $path
success (tg --url $local.url wait $process | complete)

# Wait for the remote to compact the log into a blob.
tg --url $remote.url --token $alice.token index
wait_until {
	(tg --url $remote.url --token $alice.token get $process | from json | get log?) != null
} "the remote must compact the log of a process run by the runner" --timeout 30sec

# The owner reads the compacted log across servers. Each of the process's streams is written to the corresponding stream of the log command.
let owner = tg --url $local.url log --no-timeout $process | complete
success $owner "the owner must read the compacted log of a process run by the runner."
snapshot --normalize $owner.stdout '
	loghello

'
assert ($owner.stderr | str contains 'logerror') "the owner must read the compacted stderr."

# Eve with only the process node must not read the compacted log; the log is now an object that the process node does not confer.
tg --url $remote.url --token $alice.token grant $eve.user.id process_node $process | ignore
let node_only = tg --url $remote.url --token $eve.token log $process | complete
snapshot --normalize $node_only.stdout ''

# With process_subtree_log added, Eve can read the compacted log object.
tg --url $remote.url --token $alice.token grant $eve.user.id process_subtree_log $process | ignore
let with_log = tg --url $remote.url --token $eve.token log $process | complete
snapshot --normalize $with_log.stdout '
	loghello

'
assert ($with_log.stderr | str contains 'logerror') "process_subtree_log must confer the compacted stderr."
