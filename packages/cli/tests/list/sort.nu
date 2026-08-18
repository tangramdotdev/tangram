use ../../test.nu *

# Merged results are sorted by specifier without using their locations.

let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

let id = tg --url $remote.url put 'tg.file("test")' | str trim
tg --url $remote.url tag put a $id
tg --url $local.url tag put b $id

let entries = tg --url $local.url match --no-groups --no-organizations --no-users "*" | from json
assert equal ($entries | get specifier) [a b]
assert equal ($entries | get node.options.location) [remote local]
