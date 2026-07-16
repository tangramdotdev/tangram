use ../../test.nu *

# A public build is reusable across owners: a build owned by public carries a public grant, so a different owner building the same deterministic command reuses the cached result.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact { tangram.ts: 'export default function () { return tg.file("publicsecret"); }' }

# Alice builds the command publicly, so it is not a cache hit.
let alice_build = tg --token $alice.token build --detach --verbose --public $path | from json
tg --token $alice.token wait $alice_build.process | complete
assert (($alice_build.cached? | default false) == false) "Alice's first public build should not be a cache hit."

# Eve building the identical deterministic command reuses Alice's public cached process.
let eve_build = tg --token $eve.token build --detach --verbose $path | from json
tg --token $eve.token wait $eve_build.process | complete
assert (($eve_build.cached? | default false) == true) "Eve should reuse Alice's public cached process."
