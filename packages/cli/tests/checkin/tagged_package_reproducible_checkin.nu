use std assert
use ../../test.nu *

# Create a remote server and tag the foo object on it.
let remote = spawn -n remote
let foo_path = artifact {
	contents: 'foo'
}
run tg -u $remote.url tag foo ($foo_path | path join 'contents')

# Create two local servers, both configured with the remote.
let local1 = spawn -n local1 -c {
	remotes: [{ name: default, url: $remote.url }]
}

let local2 = spawn -n local2 -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create an artifact that imports the tagged object.
let path = artifact {
	tangram.ts: '
		import * as foo from "foo";
	'
}

# Check in on the first local server.
let id1 = run tg -u $local1.url checkin $path
run tg -u $local1.url index
let output1 = run tg -u $local1.url object get --blobs --depth=inf --pretty $id1

# Check in on the second local server.
let id2 = run tg -u $local2.url checkin $path
run tg -u $local2.url index
let output2 = run tg -u $local2.url object get --blobs --depth=inf --pretty $id2

assert ($output1 == $output2) "the checkout should be reproducible across different servers."
