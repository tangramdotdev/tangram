use ../lib/test.nu *

# A push with a different exit code must not change the stored process or grant node permission.
let root_token = random chars
let remote = server spawn --cloud --name remote --config {
	authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } },
}
let alice = tg --url $remote.url login --verbose --name alice | from json
let bob = tg --url $remote.url login --verbose --name bob | from json
let local = server spawn --name local --config {
	remotes: { default: { token: $bob.token, url: $remote.url } },
}
let process = "pcs_00081061050r3gg28a1c60t3gf20"
let data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	exit: 0,
	finished_at: 1,
	host: "x86_64-linux",
	status: "finished",
}
tg --url $remote.url --token $alice.token process put $process ($data | to json)
tg process put $process ($data | upsert exit 1 | to json)
failure (tg push --eager $process | complete)
let stored = tg --url $remote.url --token $alice.token process get $process | from json
assert equal $stored.exit 0
failure (tg --url $remote.url --token $bob.token process get $process | complete)

# Pushing matching process data and children grants Bob node permission on the existing process.
let copy = server spawn --name copy --config {
	remotes: { default: { token: $bob.token, url: $remote.url } },
}
tg process put $process ($data | to json)
success (tg push --eager $process | complete)
success (tg --url $remote.url --token $bob.token process get $process | complete)
