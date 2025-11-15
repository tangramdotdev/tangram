use ../../test.nu *

# Create a remote server.
let remote_server = spawn -n remote

# Create a local server.
let local_server = spawn -n local

# Add the remote to the local server.
let output = tg remote put default $remote_server.url | complete
success $output

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.file("Hello, World!")
		}
	'
}

# Build the module.
let id = tg build $path | complete | get stdout | str trim

# Push the object.
let output = tg push $id | complete
success $output

# Confirm the object is on the remote and the same.
let local_object = tg get $id --blobs --depth=inf --pretty | complete | get stdout

let remote_object = tg --url $remote_server.url get $id --blobs --depth=inf --pretty | complete | get stdout

if $local_object != $remote_object {
	error make { msg: "objects do not match" }
}

# Index.
let output = tg index | complete
success $output

let output = tg --url $remote_server.url index | complete
success $output

# Get the metadata.
let local_metadata = tg object metadata $id --pretty | complete | get stdout

let remote_metadata = tg --url $remote_server.url object metadata $id --pretty | complete | get stdout

if $local_metadata != $remote_metadata {
	error make { msg: "metadata does not match" }
}
