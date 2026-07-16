use ../../test.nu *

# A running (not yet finalized) process's log is the live process stream. Reading it requires the log permission, just as reading the log once compacted to a blob does: the owner reads it, a principal holding only the process node is denied, and granting the log permission restores access. This guards the live-log read path, which a node-only reader must not use to read a log it could not read once compacted.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice starts a long-running process that logs a secret and then sleeps, so it stays unfinalized.
let path = artifact { tangram.ts: 'export default async function () { console.log("loghello"); await tg.sleep(60) }' }
let started = tg --token $alice.token build --detach --verbose $path | from json
let process = $started.process

# Wait until the process has logged but is still running. The owner reads her own live log.
wait_until { (tg --token $alice.token log $process | str trim | str length) > 0 } "the process should log before finishing"

# Eve with only the process node may see the process but not read its live log.
tg --token $alice.token grant $eve.user.id process_node $process | ignore
success (tg --token $eve.token get $process | complete) "Eve should see the process."
let denied = tg --token $eve.token log $process | complete
failure $denied "a node-only reader must not read a live log."

# Granting Eve the log permission restores access.
tg --token $alice.token grant $eve.user.id process_node_log $process | ignore
let allowed = tg --token $eve.token log $process | complete
snapshot --normalize $allowed.stdout '
	loghello

'

# Clean up the running process.
tg --token $alice.token cancel $process $started.lease
tg --token $alice.token wait $process | ignore
