use ../../test.nu *

# Resolving a branch tag returns the cached child within its TTL and returns the newest child when fetched with --ttl 0.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact 'Hello, World!'
let id = tg --url $remote.url checkin $path
let old = tg --url $remote.url get $id | str trim
tg --url $remote.url tag put "a/c/d" $id

# Prime the cache by resolving the branch through the local server.
let c1 = tg --url $local.url resolve "a/c" | str trim
assert equal $c1 $old "the branch should resolve to its only child"

# Add a newer child on the remote.
let path2 = artifact 'Goodbye, World!'
let id2 = tg --url $remote.url checkin $path2
let new = tg --url $remote.url get $id2 | str trim
tg --url $remote.url tag put --force "a/c/h" $id2

# Within the TTL the cached resolution is returned.
let c2 = tg --url $local.url resolve "a/c" | str trim
assert equal $c2 $c1 "within the TTL the same result should be returned"

# With --ttl 0 the newest child is returned.
let c3 = tg --url $local.url resolve --ttl 0 "a/c" | str trim
assert equal $c3 $new "with --ttl 0 the newest child should be returned"
