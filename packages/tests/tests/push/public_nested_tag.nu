use ../../util/test.nu *

# A public push of a nested tag grants anonymous read access to the tag and its target.

let remote = server spawn --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json
let root_token = random chars
let local = server spawn --name local --config {
	authentication: { root: { token: $root_token } }
	remotes: { default: { url: $remote.url, token: $alice.token } }
}
let config = mktemp
{} | to json | save -f $config

let path = artifact { 'child.txt': 'pushed publicly by a nested tag' }
let directory = tg --token $root_token checkin --no-tokens $path | referent node
tg --token $root_token tag put -p outer/inner $directory
tg --token $root_token index
tg --token $root_token push --public outer/inner
tg --url $remote.url --token $alice.token index

with-env { TANGRAM_CONFIG: $config } {
	assert equal (tg --url $remote.url tag get outer/inner | from json | get target.id) $directory
	let directory = tg --url $remote.url get --no-tokens --depth=inf --blobs $directory
	assert ($directory | str contains 'pushed publicly by a nested tag')
}
