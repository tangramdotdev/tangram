use ../../test.nu *

# tg tag put --remote pushes a tag to the configured remote server so the same tag resolves to the same item on both the local and the remote server.

# Spawn a remote and local server.
let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Tag an object on the remote server.
let tag = "foo"
let path = artifact 'foo'
tg --url $remote.url tag put $tag $path

# Tag the object on the remote server from the local server.
tg tag put --remote $tag $path

# Get tag from remote server by switching to remote context.
let remote_output = tg --url $remote.url tag get $tag | from json

assert equal $remote_output.item.kind object
assert equal $remote_output.name foo
assert equal $remote_output.specifier foo
