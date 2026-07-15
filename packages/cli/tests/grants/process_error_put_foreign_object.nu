use ../../test.nu *

# Putting a process whose error names a foreign object does not leak it: tg process put grants the putter only the process node, which does not propagate to the error object.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process that fails, so its error is stored as an object.
let alice_path = artifact { tangram.ts: 'export default function () { throw new Error("alicesecret") }' }
let alice_process = tg --token $alice.token build --detach $alice_path | str trim
tg --token $alice.token wait $alice_process | complete
let alice_error = (tg --token $alice.token get $alice_process | from json).error
assert (($alice_error | to json) | str starts-with '"err_') ("Alice's failed process should store its error as an object: " + ($alice_error | to json))

# Eve cannot read Alice's private error object before the exploit.
let denied = tg --token $eve.token get $alice_error | complete
failure $denied "Eve should not read Alice's error object before the exploit."

# Eve puts a process whose error names Alice's private error object.
let eve_process = "pcs_00081061050r3gg28a1c60t3gf20"
let process_data = {
	children: [],
	command: "cmd_01041061050r3gg28a1c60t3gf208h44rm2mb1e60s38dhr78y3wg0",
	created_at: 0,
	error: $alice_error,
	exit: 1,
	host: "test",
	sandbox: "sbx_00041061050r3gg28a1c60t3gf20",
	status: "finished",
}
tg --token $eve.token process put $eve_process ($process_data | to json)
tg --token $eve.token index

# Eve must not gain read access to Alice's error object by naming it as her process's error.
let leaked = tg --token $eve.token get $alice_error | complete
failure $leaked "Eve must not read Alice's error object after naming it as her process's error."
snapshot --normalize-ids $leaked.stdout ''
