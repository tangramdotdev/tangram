use ../../test.nu *

# Exercising a grant is akin to cloning the capability, so a principal who re-grants access to themselves retains it even after the granting principal revokes; revocation is not a global kill-switch.

let remote = spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose alice | from json
let bob = tg --url $remote.url login --verbose bob | from json

let alice_local = spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice stores a private file and grants Bob the subtree.
let directory = tg --url $alice_local.url put 'tg.directory({ "hello.txt": tg.file("hello") })' | str trim
tg --url $alice_local.url index
let file = tg --url $alice_local.url children $directory | from json | get 0
tg --url $alice_local.url push --lazy $file
tg --url $remote.url index
tg --url $remote.url --token $alice.token grant $bob.user.id object_subtree $file | ignore

# While he holds the grant, Bob re-grants the subtree to himself, cloning the capability.
tg --url $remote.url --token $bob.token grant $bob.user.id object_subtree $file | ignore

# Alice revokes the grant she made to Bob; this removes only her own grant.
tg --url $remote.url --token $alice.token revoke $bob.user.id object_subtree $file | ignore

# Bob retains access through the grant he made to himself, since a revoke cannot reach a capability another principal already holds.
let output = tg --url $remote.url --token $bob.token get $file | complete
success $output "Bob should retain access through his own grant after Alice revokes hers."
