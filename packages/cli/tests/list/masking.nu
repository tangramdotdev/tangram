use ../../test.nu *

# Listing a reference lists the children of the node selected by get.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let id = tg --url $remote.url put 'tg.file("remote")' | str trim
let remote_group = tg --url $remote.url group create foo | from json
tg --url $remote.url tag put foo/a $id
let remote_tag = tg --url $remote.url tag get foo/a | from json

let output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get foo | complete }
success $output
let group = $output.stdout | from json
assert equal $group.id $remote_group.id
assert (($group | get --optional location) == null) "get should not print a location to stdout"
assert ($output.stderr | str contains "location=remote") "the referent should include its remote location"

let children = tg --url $local.url list --no-groups foo | from json
assert equal ($children | get specifier) [foo/a]
assert equal ($children | get node.options.location) [remote]

let local_group = tg --url $local.url group create foo | from json
let output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get foo | complete }
success $output
let group = $output.stdout | from json
assert equal $group.id $local_group.id
assert (($group | get --optional location) == null) "get should not print a location to stdout"
assert ($output.stderr | str contains "location=local") "the referent should include its local location"

let output = with-env { TANGRAM_QUIET: "false" } { tg --url $local.url get foo/a | complete }
success $output
let tag = $output.stdout | from json
assert equal $tag.id $remote_tag.id
assert (($tag | get --optional location) == null) "get should not print a location to stdout"
assert ($output.stderr | str contains "location=remote") "the referent should include its remote location"
assert equal $tag.parent $remote_group.id

let children = tg --url $local.url list --no-groups foo | from json
assert equal $children []

let matches = tg --url $local.url match --no-groups "foo/*" | from json
assert equal ($matches | get specifier) [foo/a]
assert equal ($matches | get node.options.location) [remote]

let exact = tg --url $local.url match foo | from json
assert equal ($exact | get node.node) [$local_group.id]
assert equal ($exact | get node.options.location) [local]

tg --url $local.url group delete foo

let children = tg --url $local.url list --no-groups foo | from json
assert equal ($children | get specifier) [foo/a]
assert equal ($children | get node.options.location) [remote]

let matches = tg --url $local.url match --no-groups "foo/*" | from json
assert equal ($matches | get specifier) [foo/a]
assert equal ($matches | get node.options.location) [remote]
