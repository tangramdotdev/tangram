use ../../test.nu *

# Pushing a directory whose root object is missing locally but present on the remote completes and yields matching objects and metadata, under both eager and lazy push.

def test [...args] {
	# Create a remote server.
	let remote = spawn --cloud --name remote

	# Create a local server.
	let local = spawn --name local

	# Create a source server.
	let source = spawn --name source

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
	let id = tg --url $source.url build $path
	let dir_id = $id

	# Get the file id.
	let output = tg --url $source.url children $id
	let fil_id = $output | from json | get 0

	# Get the blob id.
	let output = tg --url $source.url children $fil_id
	let blb_id = $output | from json | get 0

	# Put the directory to the remote server.
	tg get --bytes $dir_id | tg --url $remote.url put --bytes --kind dir

	# Put the file to the local server.
	tg get --bytes $fil_id | tg --url $local.url put --bytes --kind fil

	# Put the blob to the local server.
	tg get --bytes $blb_id | tg --url $local.url put --bytes --kind blob

	# Confirm the directory is not on the local server.
	let output = tg --url $local.url get $dir_id | complete
	failure $output

	# Index.
	tg --url $local.url index
	tg --url $remote.url index

	# Add the remote to the local server.
	tg --url $local.url remote put default $remote.url

	# Push the directory
	tg --url $local.url push $dir_id ...$args

	# Index.
	tg --url $source.url index
	tg --url $remote.url index

	# Remove the remote.
	tg --url $local.url remote delete default

	# Confirm the object is on the remote and the same.
	let source_object = tg --url $source.url get $dir_id --blobs --depth=inf --pretty
	let remote_object = tg --url $remote.url get $dir_id --blobs --depth=inf --pretty
	assert equal $source_object $remote_object

	# Confirm the directory is not on the local server.
	let output = tg --url $local.url get $dir_id --local | complete
	failure $output

	# Confirm the object is on the remote and the same.
	let source_object = tg --url $source.url get $dir_id --blobs --depth=inf --pretty
	let remote_object = tg --url $remote.url get $dir_id --blobs --depth=inf --pretty
	assert equal $source_object $remote_object

	# Confirm metadata matches.
	let source_metadata = tg --url $source.url object metadata $id --pretty
	let remote_metadata = tg --url $remote.url object metadata $id --pretty
	assert equal $source_metadata $remote_metadata
}

test "--eager"
test "--lazy"
