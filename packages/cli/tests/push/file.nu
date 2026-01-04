use ../../test.nu *

# Create a remote server.
let remote = spawn -n remote

# Create a local server.
let local = spawn -n local

# Add the remote to the local server.
let output = tg remote put default $remote.url | complete
success $output

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.file("Hello, World!")
		}
	'
}

# Build the module.
let id = tg build $path

# Push the object.
let output = tg push $id --eager | complete
success $output

# Confirm the object is on the remote and the same.
let local_object = tg get $id --blobs --depth=inf --pretty
let remote_object = tg --url $remote.url get $id --blobs --depth=inf --pretty
assert equal $local_object $remote_object

# Index.
tg index
tg --url $remote.url index

# Get the metadata.
let local_metadata = tg object metadata $id --pretty
let remote_metadata = tg --url $remote.url object metadata $id --pretty

assert equal $local_metadata $remote_metadata
