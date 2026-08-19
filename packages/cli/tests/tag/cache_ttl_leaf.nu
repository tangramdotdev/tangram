use ../../test.nu *

# A cached leaf tag resolves to the old node within its TTL and resolves to the updated node when fetched with --ttl 0.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
let source = spawn --name source --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact 'Hello, World!'
let id = tg --url $source.url checkin $path
tg --url $source.url push $id
let old = tg --url $remote.url get $id | str trim
tg --url $remote.url tag put -p "a/b" $id

# Prime the cache by resolving the tag through the local server.
tg --url $local.url get "a/b?follow=true" | ignore

# Update the tag on the remote.
let path2 = artifact 'Goodbye, World!'
let id2 = tg --url $source.url checkin $path2
tg --url $source.url push $id2
let new = tg --url $remote.url get $id2 | str trim
tg --url $remote.url tag put -p "a/b" $id2

# Within the TTL the cached node is returned.
let cached = tg --url $local.url get "a/b?follow=true" | str trim
assert equal $cached $old "within the TTL the old node should be returned"

# With --ttl 0 the updated node is returned.
let fresh = tg --url $local.url get --ttl 0 "a/b?follow=true" | str trim
assert equal $fresh $new "with --ttl 0 the new node should be returned"
