use ../lib/test.nu *

# Checking in a package with a remote tag dependency produces the same object on two independent local servers.

# Create a remote server and tag the foo object on it.
let remote = server spawn --name remote
let foo_path = artifact {
	contents: 'foo'
}
tg --url $remote.url tag foo ($foo_path | path join 'contents')

# Create two local servers, both configured with the remote.
let local_one = server spawn --name local-one --config {
	remotes: { default: { url: $remote.url } }
}

let local_two = server spawn --name local-two --config {
	remotes: { default: { url: $remote.url } }
}

# Create an artifact that imports the tagged object.
let path = artifact {
	tangram.ts: '
		import * as foo from "foo";
	'
}

# Check in on the first local server.
let id1 = tg --url $local_one.url checkin --no-tokens $path | referent node
tg --url $local_one.url index
let output1 = tg --url $local_one.url object get --blobs --depth=inf --no-tokens --pretty $id1

# Check in on the second local server.
let id2 = tg --url $local_two.url checkin --no-tokens $path | referent node
tg --url $local_two.url index
let output2 = tg --url $local_two.url object get --blobs --depth=inf --no-tokens --pretty $id2

assert ($output1 == $output2) "the checkout should be reproducible across different servers."
