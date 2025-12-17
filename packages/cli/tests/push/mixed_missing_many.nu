use ../../test.nu *

export def test [...args] {
	# Create a remote server.
	let remote_server = spawn -n remote

	# Create a local server.
	let local_server = spawn -n local

	# Create a dummy server.
	let dummy_server = spawn -n dummy

	# Create a directory with many files to increase object count.
	let path = artifact {
		tangram.ts: '
			export default () => {
				const files = {};
				for (let i = 0; i < 50; i++) {
					files[`file_${i.toString().padStart(3, "0")}.txt`] = tg.file(`Content for file number ${i}. This is some text to make the blob larger.`);
				}
				return tg.directory(files);
			}
		'
	}

	# Build the module.
	let id = tg -u $dummy_server.url build $path
	let dir_id = $id

	# Get immediate children (files) from the directory.
	let files = tg -u $dummy_server.url children $id | from json

	# Get the blob children from each file.
	let blobs = $files | each { |fil_id|
		tg -u $dummy_server.url children $fil_id | from json
	} | flatten | uniq

	# Put the directory to the local server.
	tg -u $dummy_server.url get --bytes $dir_id | tg -u $local_server.url put --bytes -k dir

	# Put half of the files to the local server, half to the remote server (intermediate missing).
	let file_count = $files | length
	let half_files = ($file_count / 2 | math floor)
	for i in 0..<$file_count {
		let fil_id = $files | get $i
		if $i < $half_files {
			# Put to local server.
			tg -u $dummy_server.url get --bytes $fil_id | tg -u $local_server.url put --bytes -k fil
		} else {
			# Put to remote server (intermediate missing locally).
			tg -u $dummy_server.url get --bytes $fil_id | tg -u $remote_server.url put --bytes -k fil
		}
	}

	# Put half of the blobs to the local server, half to the remote server (leaf missing).
	let blob_count = $blobs | length
	let half_blobs = ($blob_count / 2 | math floor)
	for i in 0..<$blob_count {
		let blb_id = $blobs | get $i
		if $i < $half_blobs {
			# Put to local server.
			tg -u $dummy_server.url get --bytes $blb_id | tg -u $local_server.url put --bytes -k blob
		} else {
			# Put to remote server (leaf missing locally).
			tg -u $dummy_server.url get --bytes $blb_id | tg -u $remote_server.url put --bytes -k blob
		}
	}

	# Index.
	tg -u $local_server.url index
	tg -u $remote_server.url index

	# Add the remote to the local server.
	tg -u $local_server.url remote put default $remote_server.url

	# Push the directory.
	tg -u $local_server.url push $dir_id ...$args

	# Index on both servers.
	tg -u $dummy_server.url index
	tg -u $remote_server.url index

	# Confirm the metadata matches.
	let dummy_metadata = tg -u $dummy_server.url object metadata $id --pretty
	let remote_metadata = tg -u $remote_server.url object metadata $id --pretty
	assert equal $dummy_metadata $remote_metadata
}
