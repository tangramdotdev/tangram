use ../lib/test.nu *

# Putting a tag again with the same target recaptures the permissions that were established after the first put.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } }
	object: { permission_time_to_live: 60 }
	sync: { permission_time_to_live: 60 }
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let eve = tg --url $remote.url login --verbose --name eve | from json
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } }
}

# Tag the directory on the remote before it exists there, so the capture finds no permissions.
let directory = tg --url $local.url put --no-tokens 'tg.directory({"file":tg.file("later")})' | referent node
let params = { resource: $directory } | to json --raw
let advanced = tg --url $remote.url --token $root_token checkpoint watch permission_capture.advanced --params $params | from json | get watch
tg --url $remote.url --token $alice.token tag put later $directory
tg --url $remote.url --token $root_token index
timeout 30s tg --url $remote.url --token $root_token checkpoint wait permission_capture.advanced $advanced 0 | from json | ignore
tg --url $remote.url --token $root_token checkpoint continue permission_capture.advanced $advanced 0
tg --url $remote.url --token $root_token checkpoint unwatch permission_capture.advanced $advanced
let before = tg --url $remote.url --token $alice.token tag get later | from json
assert equal $before.target.id $directory
tg --url $remote.url --token $alice.token grant $bob.user.id tag_read $before.id | ignore
tg --url $remote.url --token $root_token index
failure (tg --url $remote.url --token $bob.token get --bytes $directory | complete) "the directory must not be readable before it is pushed."

# Push the directory, then put the tag again with the same target.
tg --url $local.url push $directory | ignore
tg --url $remote.url --token $root_token index
let written = tg --url $remote.url --token $root_token checkpoint watch permission_capture.written --params $params | from json | get watch
tg --url $remote.url --token $alice.token tag put later $directory
tg --url $remote.url --token $root_token index
timeout 30s tg --url $remote.url --token $root_token checkpoint wait permission_capture.written $written 0 | ignore
tg --url $remote.url --token $root_token checkpoint unwatch permission_capture.written $written
let after = tg --url $remote.url --token $alice.token tag get later | from json
assert equal $after.id $before.id

# Expire the sync permissions.
advance_time $remote 2min
tg --url $remote.url --token $root_token clean
tg --url $remote.url --token $root_token index

success (tg --url $remote.url --token $bob.token get $directory --depth inf | complete) "putting the tag again must permanently confer the pushed subtree."
failure (tg --url $remote.url --token $eve.token get --bytes $directory | complete) "a user without tag read must not read the directory."
