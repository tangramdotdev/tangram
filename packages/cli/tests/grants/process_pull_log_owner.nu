use ../lib/test.nu *

# An authorized caller can pull a finished process log even when a runner cannot read it.

let root_token = random chars
let remote = server spawn --cloud --name remote --preserve-keys --config {
	advanced: { checkpoints: true, single_process: false },
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
	roles: [api indexer scheduler],
}

let created = tg --url $remote.url --token $root_token runner create | from json
let runner = server spawn --name runner --config {
	remotes: { default: { token: $root_token, url: $remote.url } },
	roles: [api indexer runner],
	runner: { id: $created.data.id, remote: "default", token: $created.token.token },
}

let alice = tg --url $remote.url login --verbose --name alice | from json

# Alice builds a process on the remote whose stdout holds a secret.
let path = artifact { tangram.ts: 'export default function () { console.log("alicesecret"); }' }
let local_source = server spawn --name local-source --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let process = tg --url $local_source.url build --no-tokens --remote --detach $path | referent node
success (timeout 30s tg --url $local_source.url wait $process | complete) "the process should finish"

# Alice has her own server that talks to the remote as herself.
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice pulls her own process with its logs.
let pulled = timeout 30s tg --url $alice_local.url pull $process --process-log-objects | complete
success $pulled "the owner should pull their process"

# The finished log is transferred and readable locally.
let log = tg --url $alice_local.url get $process | from json | get log?
assert ($log | is-not-empty) "sync should send the finished log"
let log = timeout 30s tg --url $alice_local.url log $process --no-timeout | complete
success $log
assert equal $log.stdout "alicesecret\n"
