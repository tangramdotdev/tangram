use ../../test.nu *

export def test [...args] {
	# Create a remote server.
	let remote = spawn -n remote

	# Create a local server.
	let local = spawn -n local

	# Create a source server.
	let source = spawn -n source

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
	let id = tg -u $source.url build $path
	let dir_id = $id

	# Get the file id.
	let output = tg -u $source.url children $id
	let fil_id = $output | from json | get 0

	# Get the blob id.
	let output = tg -u $source.url children $fil_id
	let blb_id = $output | from json | get 0

	# Put the directory to the remote server.
	tg get --bytes $dir_id | tg -u $remote.url put --bytes -k dir

	# Put the file to the local server.
	tg get --bytes $fil_id | tg -u $local.url put --bytes -k fil

	# Put the blob to the local server.
	tg get --bytes $blb_id | tg -u $local.url put --bytes -k blob

	# Confirm the directory is not on the local server.
	let output = tg -u $local.url get $dir_id | complete
	failure $output

	# Index.
	tg -u $local.url index
	tg -u $remote.url index

	# Add the remote to the local server.
	tg -u $local.url remote put default $remote.url

	# Push the directory
	tg -u $local.url push $dir_id ...$args

	# Index.
	tg -u $source.url index
	tg -u $remote.url index

	# Remove the remote.
	tg -u $local.url remote delete default

	# Confirm the object is on the remote and the same.
	let source_object = tg -u $source.url get $dir_id --blobs --depth=inf --pretty
	let remote_object = tg --url $remote.url get $dir_id --blobs --depth=inf --pretty
	assert equal $source_object $remote_object

	# Confirm the directory is not on the local server.
	let output = tg -u $local.url get $dir_id --local | complete
	failure $output

	# Confirm the object is on the remote and the same.
	let source_object = tg -u $source.url get $dir_id --blobs --depth=inf --pretty
	let remote_object = tg --url $remote.url get $dir_id --blobs --depth=inf --pretty
	assert equal $source_object $remote_object

	# Confirm metadata matches.
	let source_metadata = tg -u $source.url object metadata $id --pretty
	let remote_metadata = tg -u $remote.url object metadata $id --pretty
	assert equal $source_metadata $remote_metadata
}
