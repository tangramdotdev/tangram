use ../lib/test.nu *

# Putting process data must not let a principal overwrite and claim an existing private process ID.

let root_token = random chars
let local = server spawn --config { authentication: { root: { token: $root_token }, users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

let process = "pcs_00081061050r3gg28a1c60t3gf20"
let process_data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	finished_at: 0,
	host: "alice",
	sandbox: "sbx_00041061050r3gg28a1c60t3gf20",
	status: "finished",
}

tg --token $alice.token process put $process ($process_data | to json)
tg --token $alice.token index

failure (tg --token $eve.token process get $process | complete) "Eve must not initially read Alice's private process."

# Existing IDs cannot be replaced, even by their owner or root, or with identical data.
for token in [$eve.token $alice.token $root_token] {
	for data in [$process_data ($process_data | merge { host: "replacement" })] {
		let output = tg --token $token process put $process ($data | to json) | complete
		failure $output "an existing process must not be replaced"
		assert ($output.stderr | str contains "the process already exists")
	}
}

# Rejected puts neither change the data nor grant access to the attacker.
let original = tg --token $alice.token process get $process | from json
assert equal $original.host "alice"
failure (tg --token $eve.token process get $process | complete) "a rejected put must not grant access"
