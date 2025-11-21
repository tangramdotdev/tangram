use ../../test.nu *

# Create a remote server.
let remote_server = spawn -n remote

# Create a local server.
let local_server = spawn -n local

# Add the remote to the local server.
run tg remote put default $remote_server.url

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.file("Hello, World!")
		}
	'
}

# Build the module.
let id = run tg build $path

# Push the object.
run tg push $id

# Confirm the object is on the remote and the same.
let local_object = run tg get $id --blobs --depth=inf --pretty

let remote_object = run tg --url $remote_server.url get $id --blobs --depth=inf --pretty

if $local_object != $remote_object {
	error make { msg: "objects do not match" }
}

# Index.
run tg index

run tg --url $remote_server.url index

# Get the metadata.
let local_metadata = run tg object metadata $id --pretty

let remote_metadata = run tg --url $remote_server.url object metadata $id --pretty

if $local_metadata != $remote_metadata {
	error make { msg: "metadata does not match" }
}
