use ../../test.nu *

# A cached child tag is not evicted when its parent branch is later fetched.

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
tg --url $remote.url tag put -p "a/q/r/s" $id
tg --url $remote.url tag put -p "a/q/t" $id

# Cache the child, then fetch its parent.
let s1 = tg --url $local.url get "a/q/r/s?follow=true" | str trim
tg --url $local.url get "a/q?follow=true" | ignore

# The child should still be in the cache.
let s2 = tg --url $local.url get --remote --cached "a/q/r/s?follow=true" | str trim
assert equal $s1 $s2 "the child tag should still be cached after fetching its parent"
