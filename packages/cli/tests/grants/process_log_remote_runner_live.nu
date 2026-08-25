use ../../test.nu *

# A running process on a remote runner writes its log to the remote as the process principal while the owner reads it live from her own server. Reading the live log requires the log permission, just as reading it once compacted to a blob does: the owner reads it, a principal holding only the process node is denied, and granting the log permission restores access.

let root_token = random chars

# The remote authenticates users and schedules work but holds no runner role, so the build can only complete by way of the separate runner. Log compaction is disabled so the log stays live.
let remote = server spawn --name remote --cloud --preserve-keys --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	indexer: { log_compaction: false },
	roles: [cleaner http indexer scheduler],
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

# Alice starts a long-running process on the runner that logs a secret and then sleeps, so its log stays live.
let path = artifact {
	tangram.ts: '
		export default async function () {
			console.log("loghello");
			await tg.sleep(60);
		}
	'
}
let started = tg --url $local.url build --remote --detach --verbose $path | from json
let process = $started.process

# Wait until the runner has written the log to the remote but the process is still running. The owner reads her own live log across servers.
wait_until {
	(tg --url $local.url log $process | complete | get stdout) =~ 'loghello'
} "the runner should write the log to the remote before the process finishes" --timeout 30sec
assert ((tg --url $remote.url --token $alice.token get $process | from json | get log?) == null) "the log must still be live."

# Eve with only the process node may see the process but not read its live log.
tg --url $remote.url --token $alice.token grant $eve.user.id process_node $process | ignore
success (tg --url $remote.url --token $eve.token get $process | complete) "Eve should see the process."
let denied = tg --url $remote.url --token $eve.token log $process | complete
failure $denied "a node-only reader must not read the live log of a process run by the runner."

# Granting Eve the log permission restores access.
tg --url $remote.url --token $alice.token grant $eve.user.id process_node_log $process | ignore
let allowed = tg --url $remote.url --token $eve.token log $process | complete
snapshot --normalize $allowed.stdout '
	loghello

'

# Clean up the running process.
tg --url $local.url cancel $process $started.lease
tg --url $local.url wait $process | ignore
