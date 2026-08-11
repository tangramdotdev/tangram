use ../../test.nu *

# A local node masks only a remote node with the same exact specifier, not its descendants.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let id = tg --url $remote.url put 'tg.file("remote")' | str trim
let remote_group = tg --url $remote.url group create foo | from json
tg --url $remote.url tag put foo/a $id
let remote_tag = tg --url $remote.url tag get foo/a | from json

let group = tg --url $local.url get foo | from json
assert equal $group.id $remote_group.id
assert equal $group.location remote

let children = tg --url $local.url list --no-groups foo | from json
assert equal ($children | get specifier) [foo/a]
assert equal ($children | get location) [remote]

let local_group = tg --url $local.url group create foo | from json
let group = tg --url $local.url get foo | from json
assert equal $group.id $local_group.id
assert equal $group.location local

let tag = tg --url $local.url get foo/a | from json
assert equal $tag.id $remote_tag.id
assert equal $tag.location remote
assert equal $tag.parent $remote_group.id

let children = tg --url $local.url list --no-groups foo | from json
assert equal ($children | get specifier) [foo/a]
assert equal ($children | get location) [remote]

let matches = tg --url $local.url match --no-groups "foo/*" | from json
assert equal ($matches | get specifier) [foo/a]
assert equal ($matches | get location) [remote]

let exact = tg --url $local.url match foo | from json
assert equal ($exact | get id) [$local_group.id]
assert equal ($exact | get location) [local]

tg --url $local.url group delete foo

let children = tg --url $local.url list --no-groups foo | from json
assert equal ($children | get specifier) [foo/a]
assert equal ($children | get location) [remote]

let matches = tg --url $local.url match --no-groups "foo/*" | from json
assert equal ($matches | get specifier) [foo/a]
assert equal ($matches | get location) [remote]
