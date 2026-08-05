use ../../test.nu *

# Pushing a directory whose child the pusher cannot read must fail. The remote cannot make Bob's file visible to Alice, so it requests the file, and Alice's server holds the directory shallowly and cannot send it.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let bob_local = spawn --name bob-local --config {
	remotes: { default: { url: $remote.url, token: $bob.token } },
}

# Bob creates the directory and its file, and pushes only the file.
let directory = tg --url $bob_local.url put 'tg.directory({ "b.txt": tg.file("b") })' | str trim
let file = tg --url $bob_local.url children $directory | from json | get 0
tg --url $bob_local.url index
tg --url $bob_local.url push $file
tg --url $remote.url index

# Alice's server holds the directory shallowly.
let bytes = mktemp -t
tg --url $bob_local.url object get --bytes $directory | save --force --raw $bytes
open --raw $bytes | tg --url $alice_local.url object put --bytes $directory
tg --url $alice_local.url index
let absent = tg --url $alice_local.url object get --bytes --local $file | complete
failure $absent "Alice's server must not have the file."

# Alice must not push the directory.
let pushed = tg --url $alice_local.url push $directory | complete
failure $pushed "Alice must not push a directory whose child she cannot read."
