use ../../test.nu *

# Eagerly pushing a directory of many files whose leaf blobs are all missing locally but present on the remote completes and yields matching metadata.

# Create a remote server.
let remote = spawn --cloud --name remote

# Create a local server.
let local = spawn --name local

# Create a source server.
let source = spawn --name source

# Create a directory with many files to increase object count.
let path = artifact {
	tangram.ts: '
		export default function () {
			const files = {};
			for (let i = 0; i < 50; i++) {
				files[`file_${i.toString().padStart(3, "0")}.txt`] = tg.file(`Content for file number ${i}. This is some text to make the blob larger.`);
			}
			return tg.directory(files);
		}
	'
}

# Build the module.
let id = tg --url $source.url build $path
let dir_id = $id

# Get immediate children (files) from the directory.
let files = tg --url $source.url children $id | from json

# Get the blob children from each file.
let blobs = $files | each { |fil_id|
	tg --url $source.url children $fil_id | from json
} | flatten | uniq

# Put the directory to the local server.
tg --url $source.url get --bytes $dir_id | tg --url $local.url put --bytes --kind dir

# Put all files to the local server.
for fil_id in $files {
	tg --url $source.url get --bytes $fil_id | tg --url $local.url put --bytes --kind fil
}

# Put all blobs to the remote server.
for blb_id in $blobs {
	tg --url $source.url get --bytes $blb_id | tg --url $remote.url put --bytes --kind blob
}

# Index.
tg --url $local.url index
tg --url $remote.url index

# Add the remote to the local server.
tg --url $local.url remote put default $remote.url

# Push the directory.
tg --url $local.url push $dir_id --eager

# Index on both servers.
tg --url $source.url index
tg --url $remote.url index

# Confirm the metadata matches.
let source_metadata = tg --url $source.url object metadata $id --pretty
let remote_metadata = tg --url $remote.url object metadata $id --pretty
assert equal $source_metadata $remote_metadata
snapshot $remote_metadata
