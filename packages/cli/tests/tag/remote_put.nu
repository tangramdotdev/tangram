use ../../test.nu *

# tg tag put --remote pushes a tag to the configured remote server so the same tag resolves to the same node on both the local and the remote server.

# Spawn a remote and local server.
let remote = spawn --cloud --name remote
let local = spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Tag an object on the remote server.
let tag = "foo/bar"
let path = artifact 'foo'
let id = tg --url $local.url checkin $path
tg --url $local.url push $id
tg --url $remote.url tag put -p $tag $id

# Tag the object on the remote server from the local server.
tg tag put --remote -p $tag $path

# Get the tag directly from the remote server.
let remote_output = tg --url $remote.url tag get $tag | from json

assert equal $remote_output.target.kind object
assert equal $remote_output.name bar
assert equal $remote_output.specifier foo/bar
assert equal (tg --url $remote.url group get foo | from json | get specifier) foo
