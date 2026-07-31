use ../../test.nu *

# Local items mask remote items with the same specifier, including their subtrees.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let id = tg --url $remote.url put 'tg.file("remote")' | str trim
let remote_group = tg --url $remote.url group create foo | from json
tg --url $remote.url tag put foo/a $id

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

let children = tg --url $local.url list --no-groups foo | from json
assert ($children | is-empty)

let matches = tg --url $local.url match --no-groups "foo/*" | from json
assert ($matches | is-empty)

tg --url $local.url group delete foo

let children = tg --url $local.url list --no-groups foo | from json
assert equal ($children | get specifier) [foo/a]
assert equal ($children | get location) [remote]

let matches = tg --url $local.url match --no-groups "foo/*" | from json
assert equal ($matches | get specifier) [foo/a]
assert equal ($matches | get location) [remote]
