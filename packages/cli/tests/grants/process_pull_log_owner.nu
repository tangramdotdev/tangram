use ../../test.nu *

# Sync compacts and transfers a finished process's log for an authorized caller even when background compaction is disabled.

let root_token = random chars
let remote = server spawn --cloud --name remote --preserve-keys --config {
	advanced: { single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	indexer: { log_compaction: false },
	roles: [api indexer scheduler],
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let alice = tg --url $remote.url login --verbose --name alice | from json

# Alice builds a process on the remote whose stdout holds a secret. With background compaction disabled, the log remains uncompacted until sync.
let path = artifact { tangram.ts: 'export default function () { console.log("alicesecret"); }' }
let source = server spawn --name source --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let process = tg --url $source.url build --remote --detach $path | str trim
tg --url $source.url wait $process
assert equal (tg --url $remote.url --token $alice.token get $process | from json | get log?) null

# The runner sends its indexed process data without fetching or compacting the remote log.
for mode in [--eager --lazy] {
	let destination = server spawn --name destination
	tg --url $runner.url remote put destination $destination.url
	let pushed = tg --url $runner.url push $process --remote=destination --process-logs $mode | complete
	success $pushed
	assert equal (tg --url $destination.url get $process | from json | get log?) null
	assert equal (tg --url $runner.url get $process | from json | get log?) null
	assert equal (tg --url $remote.url --token $alice.token get $process | from json | get log?) null
}

# Alice has her own server that talks to the remote as herself.
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice pulls her own process with its logs.
let pulled = tg --url $alice_local.url pull $process --process-logs | complete
success $pulled "the owner should pull their process"

# The compacted log is transferred and readable locally.
let log = tg --url $alice_local.url get $process | from json | get log?
assert ($log | is-not-empty) "sync should compact and send the log"
let log = tg --url $alice_local.url log $process --no-timeout | complete
success $log
assert equal $log.stdout "alicesecret\n"
