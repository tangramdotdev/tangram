use ../../test.nu *

# A cached leaf tag resolves to the old item within its TTL and resolves to the updated item when fetched with --ttl 0.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact 'Hello, World!'
let id = tg --url $remote.url checkin $path
let old = tg --url $remote.url get $id | str trim
tg --url $remote.url tag put "a/b" $id

# Prime the cache by resolving the tag through the local server.
tg --url $local.url resolve "a/b" | ignore

# Update the tag on the remote.
let path2 = artifact 'Goodbye, World!'
let id2 = tg --url $remote.url checkin $path2
let new = tg --url $remote.url get $id2 | str trim
tg --url $remote.url tag put --force "a/b" $id2

# Within the TTL the cached item is returned.
let cached = tg --url $local.url resolve "a/b" | str trim
assert equal $cached $old "within the TTL the old item should be returned"

# With --ttl 0 the updated item is returned.
let fresh = tg --url $local.url resolve --ttl 0 "a/b" | str trim
assert equal $fresh $new "with --ttl 0 the new item should be returned"
