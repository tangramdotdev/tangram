use ../../test.nu *

# Conflicting nodes are rejected without deleting destination subtrees.

let destination = spawn --cloud --name destination --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg --url $destination.url login --verbose --name alice | from json
let bob = tg --url $destination.url login --verbose --name bob | from json

# A conflict is rejected when the caller has group write.
let local_root = tg --url $destination.url --token $alice.token group create root-only | from json
tg --url $destination.url --token $alice.token grant $bob.user.id write root-only | ignore

# A conflict preserves a protected tag even when the caller has group admin.
let local_tree = tg --url $destination.url --token $alice.token group create tree | from json
let node = tg --url $destination.url --token $alice.token put 'tg.file("secret")' | str trim
tg --url $destination.url --token $alice.token tag put tree/secret $node
let local_tag = tg --url $destination.url --token $alice.token tag get tree/secret | from json
tg --url $destination.url --token $alice.token grant $bob.user.id admin tree | ignore

# A conflict preserves descendant groups even when the caller has parent group admin.
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
assert not equal $source_tree.id $local_tree.id
tg --url $source.url group create groups | ignore

let output = tg --url $source.url push --ancestors=always root-only | complete
failure $output "a conflicting group should be rejected"
assert ($output.stderr | str contains "the specifier is already in use")
let root = tg --url $destination.url --token $alice.token group get root-only | from json
assert equal $root.id $local_root.id
let output = tg --url $source.url push --force --ancestors=always root-only | complete
failure $output "group write should not authorize a forced replacement"
assert ($output.stderr | str contains "unauthorized")
assert equal (
	tg --url $destination.url --token $alice.token group get root-only | from json | get id
) $local_root.id

let output = tg --url $source.url push --ancestors=always tree | complete
failure $output "group admin must not authorize destructive replacement"
assert ($output.stderr | str contains "the specifier is already in use")
let tree = tg --url $destination.url --token $bob.token group get tree | from json
assert equal $tree.id $local_tree.id
success (tg --url $destination.url --token $alice.token group get $local_tree.id | complete)
success (tg --url $destination.url --token $alice.token tag get $local_tag.id | complete)

# Authorizing the conflicting root authorizes replacement of its complete subtree.
tg --url $source.url push --force --ancestors=always tree
assert equal (
	tg --url $destination.url --token $bob.token group get tree | from json | get id
) $source_tree.id
failure (
	tg --url $destination.url --token $alice.token tag get $local_tag.id | complete
) "the protected descendant should be deleted with the authorized root"

let output = tg --url $source.url push --ancestors=always groups | complete
failure $output "group admin must not authorize replacing descendant groups"
assert ($output.stderr | str contains "the specifier is already in use")
success (tg --url $destination.url --token $alice.token group get $local_groups_child.id | complete)
