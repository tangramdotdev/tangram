use ../lib/test.nu *

# A tag whose target is pushed later permanently captures the permissions the push establishes.

let root_token = random chars
let remote = server spawn --name remote --now '2026-01-01T00:00:00Z' --config {
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

# Tag the directory on the remote before it exists there, so capture finds no permissions.
let directory = tg --url $local.url put --no-tokens 'tg.directory({"file":tg.file("later")})' | referent node
tg --url $remote.url --token $alice.token tag put later $directory
tg --url $remote.url --token $root_token index
let tag = tg --url $remote.url --token $alice.token tag get later | from json
assert equal $tag.target.id $directory
tg --url $remote.url --token $alice.token grant $bob.user.id tag_read $tag.id | ignore
tg --url $remote.url --token $root_token index
failure (tg --url $remote.url --token $bob.token get --bytes $directory | complete) "the directory must not be readable before it is pushed."

# Push the directory later, then expire the sync permissions.
tg --url $local.url push $directory
tg --url $remote.url --token $root_token index
advance_time $remote 2min
tg --url $remote.url --token $root_token clean
tg --url $remote.url --token $root_token index

success (tg --url $remote.url --token $bob.token get $directory --depth inf | complete) "the tag must permanently confer the pushed subtree."
failure (tg --url $remote.url --token $eve.token get --bytes $directory | complete) "a user without tag read must not read the directory."
