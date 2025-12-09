use ../../test.nu *

# Spawn a remote and local server.
let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Tag an object on the remote server.
let tag = "foo"
let path = artifact 'foo'
tg --url $remote.url tag put $tag $path

# Tag the object on the remote server from the local server.
tg tag put -r=default $tag $path

# Get tag from local server.
let local_output = tg tag get $tag | from json

# Get tag from remote server by switching to remote context.
let remote_output = tg --url $remote.url tag get $tag | from json

# The items should be the same.
assert ($local_output.item == $remote_output.item) "the items should match"
