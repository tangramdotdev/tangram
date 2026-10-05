use ../lib/test.nu *

# A regular user cannot replace an existing process with a later compacted copy.
let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config {
	advanced: { checkpoints: true },
	remotes: { default: { token: $alice.token, url: $remote.url } },
}
let watch = tg checkpoint watch process.log.compact.read | from json | get watch
let path = artifact { tangram.ts: 'export default function () { console.log("log"); }' }
let process = tg build --no-tokens --detach $path | referent node
timeout 10s tg wait $process
timeout 10s tg checkpoint wait process.log.compact.read $watch 0 | ignore

tg push --eager $process
assert equal (tg --url $remote.url --token $alice.token get $process | from json | get log?) null

tg checkpoint unwatch process.log.compact.read $watch
timeout 10s tg index
assert ((tg get $process | from json | get log?) != null)
# Sync may skip the destination's existing record; it must not replace it.
tg push --eager --process-log-objects $process | complete | ignore
let compacted = tg get --no-tokens $process | from json
failure (tg --url $remote.url --token $alice.token process put $process ($compacted | to json) | complete)
assert equal (tg --url $remote.url --token $alice.token get $process | from json | get log?) null
