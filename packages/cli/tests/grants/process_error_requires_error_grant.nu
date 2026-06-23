use ../../test.nu *

# A failed process's error is revealed only to a principal with the error permission; a process_node grant alone masks the error.

let server = spawn --config { authentication: true }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a process that fails.
let path = artifact { tangram.ts: 'export default function () { throw tg.error("boom"); }' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process | ignore

# The owner reads the error.
let aliceget = tg --token $alice.token get $process | from json
assert ($aliceget.error? != null) "the owner should read the error."

# Alice grants Eve only process_node (basic read), not the error permission.
tg --token $alice.token grant $eve.user.id process_node $process

# Eve sees the process, but the error is masked.
let eveget = tg --token $eve.token get $process | from json
assert ($eveget.error? == null) "a process_node grant alone must not reveal the error."

# Granting Eve the error permission reveals it.
tg --token $alice.token grant $eve.user.id process_node_error $process
let eveget2 = tg --token $eve.token get $process | from json
assert ($eveget2.error? != null) "the error permission should reveal the error."
