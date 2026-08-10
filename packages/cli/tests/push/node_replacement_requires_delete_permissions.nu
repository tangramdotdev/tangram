use ../../test.nu *

# Replacing a node requires deletion permission on each minimal conflicting root.

let destination = spawn --cloud --name destination --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg --url $destination.url login --verbose alice | from json
let bob = tg --url $destination.url login --verbose bob | from json

# Group write is insufficient because deleting a group requires group admin.
let local_root = tg --url $destination.url --token $alice.token group create root-only | from json
tg --url $destination.url --token $alice.token grant $bob.user.id write root-only | ignore

# Group admin authorizes recursively deleting a protected tag in the group's subtree.
let local_tree = tg --url $destination.url --token $alice.token group create tree | from json
let item = tg --url $destination.url --token $alice.token put 'tg.file("secret")' | str trim
tg --url $destination.url --token $alice.token tag put tree/secret $item
let local_tag = tg --url $destination.url --token $alice.token tag get tree/secret | from json
tg --url $destination.url --token $alice.token grant $bob.user.id admin tree | ignore

# Group admin on a parent authorizes deleting its descendant groups.
tg --url $destination.url --token $alice.token group create groups | ignore
let local_groups_child = (
	tg --url $destination.url --token $alice.token group create groups/child | from json
)
tg --url $destination.url --token $alice.token grant $bob.user.id admin groups | ignore
tg --url $destination.url index

let source = spawn --name source --config {
	remotes: { default: { token: $bob.token, url: $destination.url } }
}
tg --url $source.url group create root-only | ignore
let source_tree = tg --url $source.url group create tree | from json
tg --url $source.url group create groups | ignore

let output = tg --url $source.url push --ancestors=always root-only | complete
failure $output "group write must not authorize destructive replacement"
let root = tg --url $destination.url --token $alice.token group get root-only | from json
assert equal $root.id $local_root.id

tg --url $source.url push --ancestors=always tree
let tree = tg --url $destination.url --token $bob.token group get tree | from json
assert equal $tree.id $source_tree.id
failure (tg --url $destination.url --token $alice.token group get $local_tree.id | complete)
failure (tg --url $destination.url --token $alice.token tag get $local_tag.id | complete)

tg --url $source.url push --ancestors=always groups
failure (
	tg --url $destination.url --token $alice.token group get $local_groups_child.id | complete
)
