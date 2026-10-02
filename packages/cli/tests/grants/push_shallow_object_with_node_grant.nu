use ../lib/test.nu *

# A source with node permission on a shallow directory can push it when the destination already has its missing child.

let remote = server spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json

# Create a directory elsewhere, then put only its child on the destination.
let local_builder = server spawn --name local-builder --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}
let directory = tg --url $local_builder.url put --no-tokens 'tg.directory({ "child.txt": tg.file("hello") })' | referent node
let child = tg --url $local_builder.url children $directory | from json | get 0
tg --url $local_builder.url index
tg --url $local_builder.url push $child
tg --url $remote.url index
let remote_directory = tg --url $remote.url --token $alice.token object get --bytes --local $directory | complete
failure $remote_directory "the destination must not initially have the directory."

# Put only the directory node on the source and grant the pusher node permission.
let local_source = server spawn --name local-source --config {
	authentication: { users: { providers: { insecure: true } } },
}
let bob = tg --url $local_source.url login --verbose --name bob | from json
let carol = tg --url $local_source.url login --verbose --name carol | from json
tg --url $local_source.url --token $carol.token remote put default $remote.url
let remote_alice = tg --url $local_source.url --token $carol.token login --remote=default --verbose --name alice | from json
assert equal $remote_alice.user.id $alice.user.id "the pusher should authenticate as the destination user."
let bytes = mktemp -t
tg --url $local_builder.url object get --bytes $directory | save --force --raw $bytes
open --raw $bytes | tg --url $local_source.url --token $bob.token object put --no-tokens --bytes $directory | referent node
tg --url $local_source.url --token $bob.token grant $carol.user.id object_node $directory | ignore
tg --url $local_source.url --token $bob.token index
let local_child = tg --url $local_source.url --token $carol.token object get --bytes --local $child | complete
failure $local_child "the source must not have the child."

# The destination supplies the missing child while accepting the directory node.
let pushed = tg --url $local_source.url --token $carol.token push $directory | complete
success $pushed "the pusher should push a shallow directory with only node permission."
let remote_directory = tg --url $remote.url --token $alice.token object get --bytes --local $directory | complete
success $remote_directory "the destination should receive the directory."
let remote_child = tg --url $remote.url --token $alice.token object get --bytes --local $child | complete
success $remote_child "the destination should retain the child."
assert equal (tg --url $remote.url --token $alice.token cat $child | str trim) "hello"
