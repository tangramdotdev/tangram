use ../../test.nu *

# A single push commits multiple nested groups and tags as one named-node batch.

let remote = server spawn --cloud --name remote --config {
	sync: { get: { database: { batch_size: 2 } } },
}
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } },
}

let first_file = tg --url $local.url put 'tg.file("first")' | str trim
let second_file = tg --url $local.url put 'tg.file("second")' | str trim
let root = tg --url $local.url group create bulk | from json
let first_group = tg --url $local.url group create bulk/first | from json
let nested_group = tg --url $local.url group create bulk/first/nested | from json
let second_group = tg --url $local.url group create bulk/second | from json
tg --url $local.url tag put bulk/first/file $first_file
tg --url $local.url tag put bulk/first/nested/file $second_file
let first_tag = tg --url $local.url tag get bulk/first/file | from json
let nested_tag = tg --url $local.url tag get bulk/first/nested/file | from json

tg --url $local.url push --group-children --tag-targets bulk

assert equal (tg --url $remote.url group get bulk | from json | get id) $root.id
assert equal (tg --url $remote.url group get bulk/first | from json | get id) $first_group.id
assert equal (tg --url $remote.url group get bulk/first/nested | from json | get id) $nested_group.id
assert equal (tg --url $remote.url group get bulk/second | from json | get id) $second_group.id
let remote_first_tag = tg --url $remote.url tag get bulk/first/file | from json
let remote_nested_tag = tg --url $remote.url tag get bulk/first/nested/file | from json
assert equal $remote_first_tag.id $first_tag.id
assert equal $remote_first_tag.target.id $first_file
assert equal $remote_nested_tag.id $nested_tag.id
assert equal $remote_nested_tag.target.id $second_file
