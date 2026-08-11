use ../../test.nu *

# Reusing a public cached process adds the process to the new owner's storage usage.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json
let path = artifact { tangram.ts: 'export default function () { return tg.file("hello"); }' }

# Alice creates a public cache entry.
let alice_spawn = tg --token $alice.token process spawn --sandbox --verbose --public $path | from json
tg --token $alice.token wait $alice_spawn.process
tg index

# Eve reuses Alice's process rather than creating one of her own.
let eve_spawn = tg --token $eve.token process spawn --sandbox --cached=true --verbose $path | from json
tg --token $eve.token wait $eve_spawn.process
assert ($eve_spawn.cached? | default false) "Eve should get a cache hit"
assert equal $eve_spawn.process $alice_spawn.process
tg index

let usage = tg --token $eve.token user usage | from json
assert ($usage.process_count >= 1) "a cache hit must charge the user for the reused process"
