use ../../test.nu *

# A stale child tag is removed from the cache when a branch is refreshed after the child is deleted on the remote.

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

# Create a branch with two children and cache it.
tg --url $remote.url tag put -p "a/k/l" $id
tg --url $remote.url tag put -p "a/k/m" $id
let k = tg --url $local.url get "a/k?follow=true" | str trim
assert equal $k $old "the branch should resolve to its newest child"

# Delete one child on the remote, then bust the cache.
tg --url $remote.url tag delete "a/k/l"
let k2 = tg --url $local.url get --ttl 0 "a/k?follow=true" | str trim
assert equal $k2 $old "the branch should still resolve via the remaining child"

# The deleted child should be gone from the cache after the refresh.
let l = tg --url $local.url get --remote --cached --no-ttl "a/k/l?follow=true" | complete
assert ($l.exit_code != 0) "the deleted child should be removed from the cache"
