use ../lib/test.nu *

# Private process metadata fields remain masked while the importing sync can answer verification requests.

let remote = server spawn --cloud --name remote --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg --url $remote.url login --verbose --name alice | from json
let eve = tg --url $remote.url login --verbose --name eve | from json

let alice_local = server spawn --name alice-local --config {
	remotes: { default: { url: $remote.url, token: $alice.token } },
}

# Alice builds a private process and pushes it to the remote.
let path = artifact {
	tangram.ts: '
		export default function () {
			return tg.file("private output")
		}
	'
}
let process = tg --url $alice_local.url build --no-tokens --detach $path | referent node
tg --url $alice_local.url wait $process
tg --url $alice_local.url index
tg --url $alice_local.url push $process
tg --url $remote.url index

# Eve cannot read the process count or output metadata through the importing sync.
let metadata = tg --url $remote.url --token $eve.token process metadata $process | from json
assert equal ($metadata | get -o subtree.count) null
assert equal ($metadata | get -o node.output_objects) null
assert equal ($metadata | get -o subtree.output_objects) null

# Alice can still read the metadata through her permissions.
let metadata = tg --url $remote.url --token $alice.token process metadata $process | from json
assert equal $metadata.subtree.count 1
assert ($metadata.node.output_objects.count > 0)
