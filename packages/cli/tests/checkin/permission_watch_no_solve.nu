use ../lib/test.nu *
use ../lib/checkin.nu reference-access

# A watched no-solve update of a cyclic directory returns a reference that still grants another user read access to its subtree.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json
let directory = artifact {
	a.tg.ts: 'import "./b.tg.ts";'
	b.tg.ts: 'import "./a.tg.ts";'
	value: 'first'
}
let first = tg --token $alice.token checkin --no-lock --root --no-solve --watch $directory | str trim
assert equal (reference-access $first --token $eve.token) subtree "the initial checkin should grant the subtree"

'second' | save --force ($directory | path join value)
tg --token $alice.token watch touch $directory ($directory | path join value)

let second = tg --token $alice.token checkin --no-lock --root --no-solve --watch $directory | str trim
assert equal (reference-access $second --token $eve.token) subtree "the update should grant the subtree"
