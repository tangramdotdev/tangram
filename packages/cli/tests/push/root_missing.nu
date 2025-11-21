use ../../test.nu *

# Create a remote server.
let remote_server = spawn -n remote

# Create a local server.
let local_server = spawn -n local

# Create a dummy server.
let dummy_server = spawn -n local

# Add the remote to the local server.
run tg remote put default $remote_server.url

let path = artifact {
	tangram.ts: '
		export default () => {
			return tg.directory({
				"hello": tg.file("Hello, World!")
			})
		}
	'
}

# Build the module.
let id = run tg -u $dummy_server.url build $path

let dir_id = $id

# Get the file id.
let output = run tg -u $dummy_server.url children $id
let fil_id = $output | from json | get 0

# Get the blob id.
let output = run tg -u $dummy_server.url children $fil_id
let blb_id = $output | from json | get 0

# Put the directory to the remote server.
tg get --bytes $dir_id | tg -u $remote_server.url put --bytes  -k dir

# Put the file to the local server.
tg get --bytes $fil_id | tg -u $remote_server.url put --bytes  -k fil

# Put the blob to the local server.
tg get --bytes $blb_id | tg -u $local_server.url put --bytes  -k blob


# Confirm the directory is not on the local server.
let output = tg -u $local_server.url get $dir_id | complete
failure $output

# Add the remote to the local server.
run tg -u $local_server.url remote put default $remote_server.url

# Push the directory
run tg -u $local_server.url push $dir_id

# Confirm the object is on the remote and the same.
let local_object = run tg -u $local_server.url get $dir_id --blobs --depth=inf --pretty

let remote_object = run tg --url $remote_server.url get $dir_id --blobs --depth=inf --pretty

if $local_object != $remote_object {
	error make { msg: "objects do not match" }
}

# Index.
run tg -u $local_server.url index

run tg -u $remote_server.url index

# Get the metadata.
let local_metadata = run tg -u $local_server.url object metadata $id --pretty

let remote_metadata = run tg -u $remote_server.url object metadata $id --pretty

if $local_metadata != $remote_metadata {
	error make { msg: "metadata does not match" }
}
