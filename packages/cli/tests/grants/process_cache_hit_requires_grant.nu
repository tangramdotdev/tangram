use ../../test.nu *

# A cache hit must not confer read access to a process the principal cannot read: building a command another principal already built returns a fresh process for the unauthorized principal, while an authorized principal still reuses the cached process.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a cacheable process.
let path = artifact { tangram.ts: 'export default () => tg.file("cachedsecret")' }
let alice_process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $alice_process | complete

# Eve cannot read Alice's process before building.
let before = tg --token $eve.token get $alice_process | complete
failure $before "Eve should not read Alice's process before building the same command."

# Alice rebuilding the same command reuses her own cached process.
let alice_again = tg --token $alice.token build --detach $path | str trim
assert ($alice_again == $alice_process) "Alice should reuse her own cached process on a second build."

# Eve building the same command must get a fresh process, not a cache hit on Alice's.
let eve_process = tg --token $eve.token build --detach $path | str trim
tg --token $eve.token wait $eve_process | complete
assert ($eve_process != $alice_process) ("Eve must not get a cache hit on Alice's process: eve=" + $eve_process + " alice=" + $alice_process)

# Eve still cannot read Alice's process after building.
let after = tg --token $eve.token get $alice_process | complete
failure $after "Eve must not read Alice's process after building the same command."

# Eve can read her own process.
let own = tg --token $eve.token get $eve_process | complete
success $own "Eve should read her own process."
