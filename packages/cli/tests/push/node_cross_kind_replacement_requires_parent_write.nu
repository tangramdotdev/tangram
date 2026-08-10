use ../../test.nu *

# Replacing a node with a different kind requires write permission on its parent.

let destination = spawn --cloud --name destination --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg --url $destination.url login --verbose alice | from json
let bob = tg --url $destination.url login --verbose bob | from json

tg --url $destination.url --token $alice.token group create parent | ignore
let item = tg --url $destination.url --token $alice.token put 'tg.file("secret")' | str trim
tg --url $destination.url --token $alice.token tag put parent/child $item
let tag = tg --url $destination.url --token $alice.token tag get parent/child | from json
tg --url $destination.url --token $alice.token grant $bob.user.id read parent | ignore
tg --url $destination.url --token $alice.token grant $bob.user.id write parent/child | ignore
tg --url $destination.url index

let source = spawn --name source --config {
	remotes: { default: { token: $bob.token, url: $destination.url } }
}
tg --url $source.url pull parent
let group = tg --url $source.url group create parent/child | from json

let output = tg --url $source.url push --ancestors=missing parent/child | complete
failure $output "tag write must not authorize creating a replacement group"
success (tg --url $destination.url --token $alice.token tag get $tag.id | complete)

tg --url $destination.url --token $alice.token grant $bob.user.id write parent | ignore
tg --url $destination.url index
tg --url $source.url push --ancestors=missing parent/child
assert equal (
	tg --url $destination.url --token $alice.token group get parent/child | from json | get id
) $group.id
failure (tg --url $destination.url --token $alice.token tag get $tag.id | complete)
