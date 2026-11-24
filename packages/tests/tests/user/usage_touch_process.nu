use ../../util/test.nu *

# Touching another user's process with a token for the process charges the toucher for the process.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } },
	object: { ttt: 0 },
	process: { ttt: 0 },
	usage: true,
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

# Alice builds a process and shares its referent with Eve.
let path = artifact { tangram.ts: 'export default function () { return tg.file("hello"); }' }
let referent = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait ($referent | referent node) | ignore
tg index
let alice_usage = tg --token $alice.token user usage | from json
assert ($alice_usage.process_count > 0) "the build should charge Alice for its process"

# Eve touches the process with Alice's token.
tg --token $eve.token process touch $referent
tg index

# Eve is charged for the process.
let eve_usage = tg --token $eve.token user usage | from json
assert equal $eve_usage.process_count $alice_usage.process_count
