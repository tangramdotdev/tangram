use ../../util/test.nu *

# Explicit public writes allow anonymous reads while ordinary writes remain private.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let config = mktemp
{} | to json | save -f $config

let path = artifact { 'child.txt': 'checked in publicly' }
let directory = tg --token $alice.token checkin --no-tokens $path | referent node
tg --token $alice.token index
let before = with-env { TANGRAM_CONFIG: $config } {
	tg --url $local.url get $directory | complete
}
failure $before "an ordinary checkin should remain private"

assert equal (tg --token $alice.token checkin --public --no-tokens $path | referent node) $directory
let file = tg --token $alice.token put --public --no-tokens 'tg.file("put publicly")' | referent node
let bytes = 0x[00 68 65 6c 6c 6f]
let blob = $bytes | tg --token $alice.token put --public --no-tokens --bytes --kind blob | referent node
let process = "pcs_00081061050r3gg28a1c60t3gf20"
let data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	exit: 0,
	host: "x86_64-linux",
	sandbox: "sbx_00041061050r3gg28a1c60t3gf20",
	status: "finished",
}
tg --token $alice.token put --public --id $process ($data | to json)
tg --token $alice.token tag put shared $directory
tg --token $alice.token tag put --public shared $directory
tg --token $alice.token index

with-env { TANGRAM_CONFIG: $config } {
	let directory = tg --url $local.url get --no-tokens --depth=inf --blobs $directory
	assert ($directory | str contains 'checked in publicly')
	let file = tg --url $local.url get --no-tokens --depth=inf --blobs $file
	assert ($file | str contains 'put publicly')
	assert equal (tg --url $local.url get --bytes $blob | into binary) $bytes
	assert equal (tg --url $local.url get $process | from json | get status) finished
	assert equal (tg --url $local.url tag get shared | from json | get target.kind) object
}
