use ../../test.nu *

export def test [...args] {
	# Create a remote server.
	let remote_server = spawn -n remote

	# Create a local server.
	let local_server = spawn -n local

	# Create a dummy server.
	let dummy_server = spawn -n dummy

	# Add the remote to the local server.
	tg remote put default $remote_server.url

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
	let id = tg -u $dummy_server.url build $path
	let dir_id = $id

	# Get the file id.
	let output = tg -u $dummy_server.url children $id
	let fil_id = $output | from json | get 0

	# Get the blob id.
	let output = tg -u $dummy_server.url children $fil_id
	let blb_id = $output | from json | get 0

	# Put the directory to the local server.
	tg get --bytes $dir_id | tg -u $local_server.url put --bytes -k dir

	# Put the file to the local server.
	tg get --bytes $fil_id | tg -u $local_server.url put --bytes -k fil

	# Put the blob to the remote server.
	tg get --bytes $blb_id | tg -u $remote_server.url put --bytes -k blob

	# Confirm the blob is not on the local server.
	let output = tg -u $local_server.url get $blb_id --blobs | complete
	failure $output

	# Add the remote to the local server.
	tg -u $local_server.url remote put default $remote_server.url

	# Push the directory
	tg -u $local_server.url push $dir_id ...$args

	# Confirm the object is on the remote and the same.
	let local_object = tg -u $local_server.url get $dir_id --blobs --depth=inf --pretty
	let remote_object = tg --url $remote_server.url get $dir_id --blobs --depth=inf --pretty
	assert equal $local_object $remote_object

	# Index.
	tg -u $local_server.url index
	tg -u $remote_server.url index

	# Confirm metadata matches.
	let local_metadata = tg -u $local_server.url object metadata $id --pretty
	let remote_metadata = tg -u $remote_server.url object metadata $id --pretty
	assert equal $local_metadata $remote_metadata
}
