use ../../test.nu *

# Pushing a directory must follow the authorization graph through an intermediate object the pusher's server does not have. Alice holds the middle directory's node and the leaf file's subtree, so the remote descends through the middle directory and resolves the leaf without requesting either.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json

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

# Bob grants Alice the middle directory's node and the leaf file's subtree.
tg --url $remote.url --token $bob.token grant $alice.user.id object_node $middle | ignore
tg --url $remote.url --token $bob.token grant $alice.user.id object_subtree $leaf | ignore

# Alice's server holds the outer directory shallowly.
let bytes = mktemp -t
tg --url $bob_local.url object get --bytes $outer | save --force --raw $bytes
open --raw $bytes | tg --url $alice_local.url object put --bytes $outer
tg --url $alice_local.url index
let absent = tg --url $alice_local.url object get --bytes --local $middle | complete
failure $absent "Alice's server must not have the middle directory."

# Alice pushes the outer directory.
let pushed = tg --url $alice_local.url push $outer | complete
success $pushed "Alice should push a directory whose subtree she can reach through her grants."

# Alice can read the directory she pushed.
let read = tg --url $remote.url --token $alice.token get $outer | complete
success $read "Alice should read the directory she pushed."
