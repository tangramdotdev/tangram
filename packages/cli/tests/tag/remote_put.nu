use std assert
use ../../test.nu *

# Create remote server.
let remote_server = spawn

# Tag an object on the remote server.
let tag = "foo"
let remote_path = artifact 'foo'
tg tag put $tag $remote_path

# Create a local server with the remote configured.
let local_server = spawn {
	remotes: [
		{ name: "default", url: $remote_server.url }
	]
}

# Tag the object on the remote server from the local server.
let local_path = artifact 'foo'
tg tag put --remote=default $tag $local_path

# Get tag from local server.
let local_output = tg tag get $tag | from json

# Get tag from remote server by switching to remote context.
with-env { TG_URL: $remote_server.url } {
	let remote_output = tg tag get $tag | from json

	# The items should be the same.
	assert ($local_output.item == $remote_output.item) 'items should match'
}
