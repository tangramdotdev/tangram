use ../../test.nu *

# Once a process is finalized its log becomes a blob object, so reading it requires a grant on that object rather than just the process node: process_node alone must not read the finalized log, but process_node plus process_subtree_log must.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a process that logs a secret and lets it finalize.
let path = artifact { tangram.ts: 'export default async function () { console.log("loghello"); return 0 }' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process
tg --token $alice.token index

# Eve with only the process node must not read the finalized log; the log is now an object that the process node does not confer.
tg --token $alice.token grant $eve.user.id process_node $process | ignore
let node_only = tg --token $eve.token log $process | complete
snapshot --normalize $node_only.stdout ''

# With process_subtree_log added, Eve can read the finalized log object.
tg --token $alice.token grant $eve.user.id process_subtree_log $process | ignore
let with_log = tg --token $eve.token log $process | complete
snapshot --normalize $with_log.stdout '
	loghello

'
