use ../lib/test.nu *

# Uploading a process must not bypass the create-only public PUT or grant access to an existing private ID.
let target = server spawn --name target --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg --url $target.url login --verbose --name alice | from json
let eve = tg --url $target.url login --verbose --name eve | from json
let process = "pcs_00081061050r3gg28a1c60t3gf20"
let data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	finished_at: 0,
	host: "alice",
	status: "finished",
}
tg --url $target.url --token $alice.token process put $process ($data | to json)
tg --url $target.url --token $alice.token index

# Even knowledge of identical process data does not authorize claiming the ID.
for mode in [--eager --lazy] {
	for host in [alice eve] {
		let source = server spawn --name $'source-($mode)-($host)' --config {
			remotes: { default: { url: $target.url, token: $eve.token } }
		}
		tg --url $source.url process put $process ($data | merge { host: $host } | to json)
		let pushed = tg --url $source.url push $process $mode | complete
		failure $pushed "sync must reject an existing process that the caller cannot access"
		failure (tg --url $target.url --token $eve.token process get $process | complete) "a rejected sync must not grant access"
		let original = tg --url $target.url --token $alice.token process get $process | from json
		assert equal $original.host "alice"
	}
}

# Repeated uploads of a caller's own finished process remain valid.
let source = server spawn --name legitimate --config {
	remotes: { default: { url: $target.url, token: $eve.token } }
}
let own_process = "pcs_00041061050r3gg28a1c60t3gf20"
tg --url $source.url process put $own_process ($data | merge { host: "eve" } | to json)
for mode in [--eager --lazy --eager] {
	success (tg --url $source.url push $own_process $mode | complete)
	let own = tg --url $target.url --token $eve.token process get $own_process | from json
	assert equal $own.host "eve"
}
