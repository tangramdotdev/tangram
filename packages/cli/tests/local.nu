use ../test.nu *

# The local flag restricts get and object metadata to the local server, failing when the object is only on a remote, while omitting the flag fetches it from the remote.

# Create a remote server.
let remote = spawn --cloud --name remote

# Create a local server.
let local = spawn --name local

# Create a source server.
let source = spawn --name source

# Create and build a simple artifact on the source server.
let path = artifact {
	tangram.ts: 'export default function () { return tg.file("test content"); }'
}
let id = tg --url $source.url build $path

# Put the object only on the remote server.
tg --url $source.url get --bytes $id | tg --url $remote.url put --bytes --kind fil

# Index the remote server.
tg --url $remote.url index

# Add the remote to the local server.
tg --url $local.url remote put default $remote.url

# Test 1: tg get --local should fail because the object is not on the local server.
let output = tg --url $local.url get $id --local | complete
failure $output

# Test 2: tg get without --local should succeed because it fetches from the remote.
let output = tg --url $local.url get $id | complete
success $output

# Remove the object from the local server to reset for metadata tests.
tg --url $local.url clean

# Test 3: tg object metadata --local should fail because the object is not on the local server.
let output = tg --url $local.url object metadata $id --local | complete
failure $output

# Test 4: tg object metadata without --local should succeed because it fetches from the remote.
let output = tg --url $local.url object metadata $id | complete
success $output
