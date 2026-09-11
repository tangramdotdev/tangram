use ../../test.nu *

# Sync omits a live process log even when the caller is authorized to read it. The remote's log compaction task is disabled so the log stays live during the pull.

let root_token = random chars
let remote = server spawn --cloud --name remote --preserve-keys --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	indexer: { log_compaction: false },
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $created.token.token, url: $remote.url } },
	roles: [indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let alice = tg --url $remote.url login --verbose --name alice | from json

# Alice builds a process on the remote whose stdout holds a secret. With log compaction disabled the log stays live (data.log is null).
let path = artifact { tangram.ts: 'export default function () { console.log("alicesecret"); }' }
let source = server spawn --name source --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let process = tg --url $source.url build --remote --detach $path | str trim
wait_until { (tg --url $remote.url --token $alice.token process status $process | from json | get 0) == "finished" } --timeout 30sec
wait_until { (tg --url $remote.url --token $alice.token process log $process | complete | get stdout | str trim) == "alicesecret" } --timeout 30sec

# Alice has her own server that talks to the remote as herself.
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice pulls her own process with its logs.
let pulled = tg --url $alice_local.url pull $process --process-logs | complete
success $pulled "the owner should pull their process"

# The live log is not transferred.
let log = tg --url $alice_local.url get $process | from json | get log?
assert ($log == null) "the uncompacted log should not be sent"
