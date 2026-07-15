use ../../test.nu *

# Cache reuse is owner-scoped: a different owner building the same deterministic command does not reuse the first owner's private cached process. Cross-principal reuse requires a public build.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact { tangram.ts: 'export default function () { return tg.file("cachedsecret"); }' }

# Alice's first build is not a cache hit.
let alice_build = tg --token $alice.token build --detach --verbose $path | from json
tg --token $alice.token wait $alice_build.process | complete
assert (($alice_build.cached? | default false) == false) "Alice's first build should not be a cache hit."

# Eve building the identical deterministic command must not reuse Alice's private process.
let eve_build = tg --token $eve.token build --detach --verbose $path | from json
tg --token $eve.token wait $eve_build.process | complete
assert (($eve_build.cached? | default false) == false) "Eve must not reuse Alice's private cached process across owners."
assert ($eve_build.process != $alice_build.process) "Eve must build her own fresh process, not adopt Alice's."
