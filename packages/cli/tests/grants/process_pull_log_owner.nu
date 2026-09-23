use ../lib/test.nu *

# An authorized caller can pull a compacted process log even when a runner cannot read it.

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
let source = server spawn --name source --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let watch = tg --url $remote.url --token $root_token checkpoint watch process.log.compact.read | from json | get watch
let process = tg --url $source.url build --remote --detach $path | str trim
tg --url $source.url wait $process
let hit = tg --url $remote.url --token $root_token checkpoint wait process.log.compact.read $watch 0 | from json
assert equal $hit.params.process $process

# The runner sends its indexed process data without fetching or compacting the remote log.
for mode in [--eager --lazy] {
	let destination = server spawn --name destination
	tg --url $runner.url remote put destination $destination.url
	let pushed = tg --url $runner.url push $process --remote=destination --process-logs $mode | complete
	success $pushed
	assert equal (tg --url $destination.url get $process | from json | get log?) null
	assert equal (tg --url $runner.url get $process | from json | get log?) null
}

tg --url $remote.url --token $root_token checkpoint unwatch process.log.compact.read $watch
tg --url $remote.url --token $root_token index

# Alice has her own server that talks to the remote as herself.
let alice_local = server spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice pulls her own process with its logs.
let pulled = tg --url $alice_local.url pull $process --process-logs | complete
success $pulled "the owner should pull their process"

# The compacted log is transferred and readable locally.
let log = tg --url $alice_local.url get $process | from json | get log?
assert ($log | is-not-empty) "sync should send the compacted log"
let log = tg --url $alice_local.url log $process --no-timeout | complete
success $log
assert equal $log.stdout "alicesecret\n"
