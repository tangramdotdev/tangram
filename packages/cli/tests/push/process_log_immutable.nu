use ../lib/test.nu *

# A finished log is present on the first push and remains immutable for ordinary users.
let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let local = server spawn --name local --config { remotes: { default: { token: $alice.token, url: $remote.url } } }
let path = artifact { tangram.ts: 'export default function () { console.log("log"); }' }
let process = tg build --no-tokens --detach $path | referent node
timeout 10s tg wait $process
let data = tg get --no-tokens $process | from json
assert ($data.log? | is-not-empty)
tg push --eager $process
assert equal (tg --url $remote.url --token $alice.token get --no-tokens $process | from json | get log) $data.log
tg push --eager --process-log-objects $process
tg --url $remote.url --token $alice.token process put $process ($data | to json)
failure (tg --url $remote.url --token $alice.token process put $process ($data | reject log | to json) | complete)
let other = tg write "another log" | referent node
failure (tg --url $remote.url --token $alice.token process put $process ($data | upsert log $other | to json) | complete)
assert equal (tg --url $remote.url --token $alice.token get --no-tokens $process | from json | get log) $data.log
let output = tg --url $remote.url --token $alice.token log --no-timeout $process | complete
success $output
assert equal $output.stdout "log\n"
