use ../../test.nu *

# A source with node permission on a shallow directory can push it when the destination already has its missing child.

let remote = server spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let remote_user = tg --url $remote.url login --verbose --name remote-user | from json

# Create a directory elsewhere, then put only its child on the destination.
let builder = server spawn --name builder --config {
	remotes: { default: { url: $remote.url, token: $remote_user.token } },
}
let directory = tg --url $builder.url put 'tg.directory({ "child.txt": tg.file("hello") })' | str trim
let child = tg --url $builder.url children $directory | from json | get 0
tg --url $builder.url index
tg --url $builder.url push $child
tg --url $remote.url index
let remote_directory = tg --url $remote.url --token $remote_user.token object get --bytes --local $directory | complete
failure $remote_directory "the destination must not initially have the directory."

# Put only the directory node on the source and grant the pusher node permission.
let source = server spawn --name source --config {
	authentication: { users: { providers: { insecure: true } } },
}
let owner = tg --url $source.url login --verbose --name owner | from json
let pusher = tg --url $source.url login --verbose --name pusher | from json
tg --url $source.url --token $pusher.token remote put default $remote.url
let pusher_remote = tg --url $source.url --token $pusher.token login --remote=default --verbose --name remote-user | from json
assert equal $pusher_remote.user.id $remote_user.user.id "the pusher should authenticate as the destination user."
let bytes = mktemp -t
tg --url $builder.url object get --bytes $directory | save --force --raw $bytes
open --raw $bytes | tg --url $source.url --token $owner.token object put --bytes $directory
tg --url $source.url --token $owner.token grant $pusher.user.id object_node $directory | ignore
tg --url $source.url --token $owner.token index
let local_child = tg --url $source.url --token $pusher.token object get --bytes --local $child | complete
failure $local_child "the source must not have the child."

# The destination supplies the missing child while accepting the directory node.
let pushed = tg --url $source.url --token $pusher.token push $directory | complete
success $pushed "the pusher should push a shallow directory with only node permission."
let remote_directory = tg --url $remote.url --token $remote_user.token object get --bytes --local $directory | complete
success $remote_directory "the destination should receive the directory."
let remote_child = tg --url $remote.url --token $remote_user.token object get --bytes --local $child | complete
success $remote_child "the destination should retain the child."
assert equal (tg --url $remote.url --token $remote_user.token cat $child | str trim) "hello"
