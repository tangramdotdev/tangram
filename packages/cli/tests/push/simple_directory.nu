use ../../test.nu *

# Create a remote server.
let remote = spawn --cloud -n remote

# Create a local server.
let local = spawn -n local

# Add the remote.
let output = tg remote put default $remote.url | complete
success $output

# Create some test content.
let path = artifact {
	tangram.ts: r#'
		export default () => {
			return tg.directory({
				"hello.txt": tg.file("Hello, world!"),
				"subdirectory": tg.directory({
					"nested.txt": tg.file("I'm nested!")
				})
			})
		}
	'#
}

# Build the module.
let id = tg build $path

# Push with optional flags.
let output = (tg push $id --eager) | complete
success $output

# Confirm object is identical locally and remotely.
let local_object = tg get $id --blobs --depth=inf --pretty
let remote_object = tg --url $remote.url get $id --blobs --depth=inf --pretty

assert equal $local_object $remote_object

# Index.
success (tg --url $local.url index | complete)
success (tg --url $remote.url index | complete)

# Confirm metadata matches.
let local_metadata  = tg --url $local.url object metadata $id --pretty
let remote_metadata = tg --url $remote.url object metadata $id --pretty
assert equal $local_metadata $remote_metadata
