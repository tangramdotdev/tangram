use ../../test.nu *

# Pushing a directory must stop at an intermediate object the pusher cannot read, even when she can read the leaf beneath it. The remote cannot make the middle directory visible to Alice, so it requests it, and Alice's server holds the outer directory shallowly and cannot send it.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let bob_local = spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}

# Bob creates the three objects and pushes the middle directory.
let outer = tg --url $bob_local.url put 'tg.directory({ "b": tg.directory({ "c.txt": tg.file("c") }) })' | str trim
let middle = tg --url $bob_local.url children $outer | from json | get 0
let leaf = tg --url $bob_local.url children $middle | from json | get 0
tg --url $bob_local.url index
tg --url $bob_local.url push $middle
tg --url $remote.url index

# Bob grants Alice the leaf file's subtree but nothing on the middle directory.
tg --url $remote.url --token $bob.token grant $alice.user.id object_subtree $leaf | ignore

# Alice's server holds the outer directory shallowly.
let bytes = mktemp -t
tg --url $bob_local.url object get --bytes $outer | save --force --raw $bytes
open --raw $bytes | tg --url $alice_local.url object put --bytes $outer
tg --url $alice_local.url index
let absent = tg --url $alice_local.url object get --bytes --local $middle | complete
failure $absent "Alice's server must not have the middle directory."

# Alice must not push the outer directory.
let pushed = tg --url $alice_local.url push $outer | complete
failure $pushed "Alice must not push a directory whose intermediate object she cannot read."
