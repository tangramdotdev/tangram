use ../lib/test.nu *

# A tag whose process target is pushed later permanently captures the process permissions the push establishes.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	object: { permission_time_to_live: 60 }
	process: { permission_time_to_live: 60 }
	sync: { permission_time_to_live: 60 }
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let eve = tg --url $remote.url login --verbose --name eve | from json
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } }
}

# Build locally, then tag the process on the remote before it exists there.
let path = artifact { tangram.ts: 'export default function () { return tg.file("later"); }' }
let process = tg --url $local.url build --no-tokens --detach $path | referent node
let finished = tg --url $local.url wait $process | from json
assert equal $finished.exit 0
let output = $finished.output.value | split row '?' | first
tg --url $local.url index

# Finish the initial capture attempt before pushing the target.
let params = { resource: $process } | to json --raw
let advanced = tg --url $remote.url --token $root_token checkpoint watch permission_capture.advanced --params $params | from json | get watch
tg --url $remote.url --token $alice.token tag put later $process
tg --url $remote.url --token $root_token index
timeout 30s tg --url $remote.url --token $root_token checkpoint wait permission_capture.advanced $advanced 0 | from json | ignore
tg --url $remote.url --token $root_token checkpoint continue permission_capture.advanced $advanced 0
tg --url $remote.url --token $root_token checkpoint unwatch permission_capture.advanced $advanced
let tag = tg --url $remote.url --token $alice.token tag get later | from json
assert equal $tag.target.id $process
tg --url $remote.url --token $alice.token grant $bob.user.id tag_read $tag.id | ignore
tg --url $remote.url --token $root_token index
failure (tg --url $remote.url --token $bob.token get $process | complete) "the process must not be readable before it is pushed."

# Push the process later, then expire the sync permissions.
let written = tg --url $remote.url --token $root_token checkpoint watch permission_capture.written --params $params | from json | get watch
tg --url $local.url push $process | ignore
tg --url $remote.url --token $root_token index
timeout 30s tg --url $remote.url --token $root_token checkpoint wait permission_capture.written $written 0 | ignore
tg --url $remote.url --token $root_token checkpoint unwatch permission_capture.written $written
advance_time $remote 2min
tg --url $remote.url --token $root_token clean
tg --url $remote.url --token $root_token index

let destination = tg --url $remote.url --token $bob.token get $process | from json
assert equal $destination.status finished
success (tg --url $remote.url --token $bob.token get --bytes $output | complete) "the tag must permanently confer the pushed output."
failure (tg --url $remote.url --token $eve.token get $process | complete) "a user without tag read must not read the process."
