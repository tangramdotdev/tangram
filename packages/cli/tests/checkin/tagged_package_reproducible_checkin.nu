use ../../test.nu *

# Checking in a package with a remote tag dependency produces the same object on two independent local servers.

# Create a remote server and tag the foo object on it.
let remote = spawn --name remote
let foo_path = artifact {
	contents: 'foo'
}
tg --url $remote.url tag foo ($foo_path | path join 'contents')

# Create two local servers, both configured with the remote.
let local1 = spawn --name local1 --config {
	remotes: { default: { url: $remote.url } }
}

let local2 = spawn --name local2 --config {
	remotes: { default: { url: $remote.url } }
}

# Create an artifact that imports the tagged object.
let path = artifact {
	tangram.ts: '
		import * as foo from "foo";
	'
}

# Check in on the first local server.
let id1 = tg --url $local1.url checkin $path
tg --url $local1.url index
let output1 = tg --url $local1.url object get --blobs --depth=inf --pretty $id1

# Check in on the second local server.
let id2 = tg --url $local2.url checkin $path
tg --url $local2.url index
let output2 = tg --url $local2.url object get --blobs --depth=inf --pretty $id2

assert ($output1 == $output2) "the checkout should be reproducible across different servers."
