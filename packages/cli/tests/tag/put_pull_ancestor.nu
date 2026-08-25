use ../../test.nu *

# Putting a nested tag pulls a remote ancestor, then creates the remaining ancestors.

let remote = server spawn --cloud --name remote --config {
	sync: { put: { resolve: { batch_size: 32, batch_timeout: 0 } } }
}
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let remote_root = tg --url $remote.url group create parent | from json
let node = tg --url $local.url put 'tg.file("data")' | str trim
tg --url $local.url tag put -p parent/child/tag $node
let local_root = tg --url $local.url group get parent | from json
let local_parent = tg --url $local.url group get parent/child | from json
let tag = tg --url $local.url tag get parent/child/tag | from json

assert equal $local_root.id $remote_root.id
assert equal $local_parent.parent $remote_root.id
assert equal $tag.target.id $node
