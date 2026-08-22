use ../../test.nu *

# A node cannot replace a different kind at the same specifier without force.

let destination = spawn --cloud --name destination --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg --url $destination.url login --verbose --name alice | from json
let bob = tg --url $destination.url login --verbose --name bob | from json

tg --url $destination.url --token $alice.token group create parent | ignore
let node = tg --url $destination.url --token $alice.token put 'tg.file("secret")' | str trim
tg --url $destination.url --token $alice.token tag put parent/child $node
let tag = tg --url $destination.url --token $alice.token tag get parent/child | from json
tg --url $destination.url --token $alice.token grant $bob.user.id read parent | ignore
tg --url $destination.url --token $alice.token grant $bob.user.id write parent/child | ignore
tg --url $destination.url index

let source = spawn --name source --config {
	remotes: { default: { token: $bob.token, url: $destination.url } }
}
tg --url $source.url pull parent
let group = tg --url $source.url group create parent/child | from json
assert not equal $group.id $tag.id

let output = tg --url $source.url push --ancestors=missing parent/child | complete
failure $output "a group must not replace a tag at the same specifier"
assert ($output.stderr | str contains "the specifier is already in use")
success (tg --url $destination.url --token $alice.token tag get $tag.id | complete)

let output = tg --url $source.url push --force --ancestors=missing parent/child | complete
failure $output "tag write without parent write should not authorize a cross-kind replacement"
assert ($output.stderr | str contains "unauthorized")

tg --url $destination.url --token $alice.token grant $bob.user.id write parent | ignore
tg --url $destination.url index
let output = tg --url $source.url push --ancestors=missing parent/child | complete
failure $output "parent write must not authorize replacing a tag with a group"
assert ($output.stderr | str contains "the specifier is already in use")
assert equal (
	tg --url $destination.url --token $alice.token tag get parent/child | from json | get id
) $tag.id
success (tg --url $destination.url --token $alice.token tag get $tag.id | complete)

tg --url $source.url push --force --ancestors=missing parent/child
assert equal (
	tg --url $destination.url --token $bob.token group get parent/child | from json | get id
) $group.id
failure (
	tg --url $destination.url --token $alice.token tag get $tag.id | complete
) "the conflicting tag should be deleted"
