use ../../lib/test.nu *
use ../../lib/checkin.nu reference-access

# A newly created pointer artifact's reference grants another user read access to its graph subtree.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json
let directory = artifact {
	a.tg.ts: 'import "./b.tg.ts";'
	b.tg.ts: 'import "./a.tg.ts";'
}
let path = $directory | path join a.tg.ts
let reference = tg --token $alice.token checkin --no-lock --root $path | str trim
assert equal (reference-access $reference --token $eve.token) subtree "the pointer artifact reference should grant its subtree"
