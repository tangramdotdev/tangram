use ../../util/test.nu *

# Push can publish a private tag and a bare object on an authenticated destination.

let remote = server spawn --name remote --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $remote.url login --verbose --name alice | from json
let root_token = random chars
let local = server spawn --name local --config {
	authentication: { root: { token: $root_token } }
	remotes: { default: { url: $remote.url, token: $alice.token } }
}
let config = mktemp
{} | to json | save -f $config

let path = artifact { 'child.txt': 'pushed publicly by tag' }
let directory = tg --token $root_token checkin --no-tokens $path | referent node
let file = tg --token $root_token put --no-tokens 'tg.file("pushed publicly by ID")' | referent node
tg --token $root_token tag put shared $directory
tg --token $root_token index
tg --token $root_token push shared $file
tg --url $remote.url --token $alice.token index
with-env { TANGRAM_CONFIG: $config } {
	failure (tg --url $remote.url tag get shared | complete) "a push defaults to a private tag"
	failure (tg --url $remote.url get $file | complete) "a push defaults to a private object"
}

tg --token $root_token push --public shared $file
tg --url $remote.url --token $alice.token index
with-env { TANGRAM_CONFIG: $config } {
	assert equal (tg --url $remote.url tag get shared | from json | get target.id) $directory
	let directory = tg --url $remote.url get --no-tokens --depth=inf --blobs $directory
	assert ($directory | str contains 'pushed publicly by tag')
	let file = tg --url $remote.url get --no-tokens --depth=inf --blobs $file
	assert ($file | str contains 'pushed publicly by ID')
}
