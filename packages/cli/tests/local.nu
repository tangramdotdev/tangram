use ../test.nu *

# Create a remote server.
let remote = spawn --cloud -n remote

# Create a local server.
let local = spawn -n local

# Create a source server.
let source = spawn -n source

# Create and build a simple artifact on the source server.
let path = artifact {
	tangram.ts: 'export default () => tg.file("test content");'
}
let id = tg -u $source.url build $path

# Put the object only on the remote server.
tg -u $source.url get --bytes $id | tg -u $remote.url put --bytes -k fil

# Index the remote server.
tg -u $remote.url index

# Add the remote to the local server.
tg -u $local.url remote put default $remote.url

# Test 1: tg get --local should fail because the object is not on the local server.
let output = tg -u $local.url get $id --local | complete
failure $output

# Test 2: tg get without --local should succeed because it fetches from the remote.
let output = tg -u $local.url get $id | complete
success $output

# Remove the object from the local server to reset for metadata tests.
tg -u $local.url clean

# Test 3: tg object metadata --local should fail because the object is not on the local server.
let output = tg -u $local.url object metadata $id --local | complete
failure $output

# Test 4: tg object metadata without --local should succeed because it fetches from the remote.
let output = tg -u $local.url object metadata $id | complete
success $output
