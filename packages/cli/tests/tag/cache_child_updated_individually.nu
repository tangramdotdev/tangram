use ../../test.nu *

# A child tag cached as part of a parent branch fetch can later be updated individually with --ttl 0.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact 'Hello, World!'
let id = tg --url $remote.url checkin $path
tg --url $remote.url tag put "a/b" $id

# Cache the parent's children, including a/b.
tg --url $local.url resolve --ttl 0 "a" | ignore

# Update the child on the remote.
let path2 = artifact 'Final version'
let id2 = tg --url $remote.url checkin $path2
let new = tg --url $remote.url get $id2 | str trim
tg --url $remote.url tag put --force "a/b" $id2

# Busting the cache for the child specifically returns the new item.
let b = tg --url $local.url resolve --ttl 0 "a/b" | str trim
assert equal $b $new "the child updated individually should return the new item"
