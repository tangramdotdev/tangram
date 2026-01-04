use ../../test.nu *

# Create a remote server.
let remote = spawn --cloud -n remote

# Create a local server.
let local = spawn -n local

# Create a source server.
let source = spawn -n source

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
let id = tg -u $source.url build $path
let dir_id = $id

# Get immediate children (files) from the directory.
let files = tg -u $source.url children $id | from json

# Get the blob children from each file.
let blobs = $files | each { |fil_id|
	tg -u $source.url children $fil_id | from json
} | flatten | uniq

# Put the directory to the local server.
tg -u $source.url get --bytes $dir_id | tg -u $local.url put --bytes -k dir

# Put all files to the local server.
for fil_id in $files {
	tg -u $source.url get --bytes $fil_id | tg -u $local.url put --bytes -k fil
}

# Put all blobs to the remote server.
for blb_id in $blobs {
	tg -u $source.url get --bytes $blb_id | tg -u $remote.url put --bytes -k blob
}

# Index.
tg -u $local.url index
tg -u $remote.url index

# Add the remote to the local server.
tg -u $local.url remote put default $remote.url

# Push the directory.
tg -u $local.url push $dir_id --eager

# Index on both servers.
tg -u $source.url index
tg -u $remote.url index

# Confirm the metadata matches.
let source_metadata = tg -u $source.url object metadata $id --pretty
let remote_metadata = tg -u $remote.url object metadata $id --pretty
assert equal $source_metadata $remote_metadata
snapshot $remote_metadata
