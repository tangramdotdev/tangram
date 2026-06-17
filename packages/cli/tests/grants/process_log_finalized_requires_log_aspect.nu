use ../../test.nu *

# Once a process is finalized its log becomes a blob object, so reading it requires the log aspect: process_node alone must not read it, but process_node plus process_subtree_log must.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a process that logs a secret and lets it finalize.
let path = artifact { tangram.ts: 'export default async () => { console.log("loghello"); return 0 }' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process

# Wait until the process is finalized, which sets its log object.
wait_until { (tg --token $alice.token get $process | from json | get log) != null } "the process should finalize"

# Eve with only the process node must not read the finalized log; the log is now an object that requires the log aspect.
tg --token $alice.token grant $eve.user.id process_node $process | ignore
let node_only = tg --token $eve.token log $process | complete
assert (not ($node_only.stdout | str contains "loghello")) ("process_node alone must not read a finalized log: " + ($node_only | to json))

# With the log aspect added, Eve can read the finalized log object.
tg --token $alice.token grant $eve.user.id process_subtree_log $process | ignore
let with_log = tg --token $eve.token log $process | complete
assert ($with_log.stdout | str contains "loghello") ("process_node plus process_subtree_log should read the finalized log: " + ($with_log | to json))
