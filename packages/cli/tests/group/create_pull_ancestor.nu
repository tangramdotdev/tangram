use ../../test.nu *

# Creating a nested group pulls an existing remote parent by default.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let remote_root = tg --url $remote.url group create parent | from json
let remote_parent = tg --url $remote.url group create parent/child | from json
let child = tg --url $local.url group create parent/child/grandchild | from json
let local_root = tg --url $local.url group get parent | from json
let local_parent = tg --url $local.url group get parent/child | from json

assert equal $child.specifier parent/child/grandchild
assert equal $local_root.id $remote_root.id
assert equal $local_parent.id $remote_parent.id
