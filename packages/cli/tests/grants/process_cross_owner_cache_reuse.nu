use ../../test.nu *

# Cross-owner reuse of an identical deterministic command is a known authorization gap. A process built by one owner should be reusable by another owner that builds the same deterministic command, because the output is a pure function of the command and is therefore reproducible. Today the second owner re-executes the command instead of reusing the cached result. The reuse must not confer read access to the first owner's process record (see process_cache_hit_requires_grant.nu); the second owner should receive the reproducible output in a fresh process, so this test asserts a cache hit while keeping the two processes distinct.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

let path = artifact { tangram.ts: 'export default function () { return tg.file("cachedsecret"); }' }

# Alice builds the cacheable process for the first time, so it is not a cache hit. The cached field is omitted from the output when false, so default it.
let alice_build = tg --token $alice.token build --detach --verbose $path | from json
tg --token $alice.token wait $alice_build.process | complete
assert (($alice_build.cached? | default false) == false) "Alice's first build should not be a cache hit."

# Eve building the identical deterministic command should reuse the cached result rather than re-executing it.
let eve_build = tg --token $eve.token build --detach --verbose $path | from json
tg --token $eve.token wait $eve_build.process | complete
assert (($eve_build.cached? | default false) == true) "Eve should reuse the cached result of the identical deterministic command rather than re-executing it."

# The reuse must give Eve a fresh process rather than Alice's, so that a cache hit does not confer read access to Alice's process record.
assert ($eve_build.process != $alice_build.process) "Eve must reuse the output in a fresh process, not adopt Alice's process record."
