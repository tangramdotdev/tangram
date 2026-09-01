use ../../test.nu *

# The owner may still pull and read a live process log. The compaction gate that withholds the log from an unauthorized puller must not withhold it from the process owner, who is authorized to read it. The remote's log compaction task is disabled so the log stays live, exercising the on-demand compaction path during the pull.

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

# Alice pulls her own process with its logs. The on-demand compaction must run for her, since she is authorized to read her own log.
let pulled = tg --url $alice_local.url pull $process --process-logs | complete
success $pulled "the owner should pull their process log"

# Alice reads her own log on her own server.
let alice_log = tg --url $alice_local.url process log $process | complete
assert equal ($alice_log.stdout | str trim) "alicesecret"
