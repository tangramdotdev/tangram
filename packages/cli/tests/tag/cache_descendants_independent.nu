use ../../test.nu *

# A descendant tag can be fetched and cached independently of the siblings already cached under the same parent.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}
let source = server spawn --name source --config {
	remotes: { default: { url: $remote.url } }
}

let path = artifact 'Hello, World!'
let id = tg --url $source.url checkin $path
tg --url $source.url push $id
let old = tg --url $remote.url get $id | str trim

# Cache one deep descendant.
tg --url $remote.url tag put -p "a/c/e/f/g" $id
tg --url $local.url get "a/c/e/f/g?follow=true" | ignore

# Add a sibling branch on the remote and fetch it.
tg --url $remote.url tag put -p "a/c/e/i/j" $id
let e = tg --url $local.url get --ttl 0 "a/c/e/i/j?follow=true" | str trim
assert equal $e $old "the new descendant should resolve through the local server"
