use ../../test.nu *

# A running (not yet finalized) process's log is the live process stream, which the process node covers, so process_node alone reads it.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice starts a long-running process that logs a secret and then sleeps, so it stays unfinalized.
let path = artifact { tangram.ts: 'export default async function () { console.log("loghello"); await tg.sleep(60) }' }
let started = tg --token $alice.token build --detach --verbose $path | from json
let process = $started.process

# Wait until the process has logged but is still running.
wait_until { (tg --token $alice.token log $process | str trim | str length) > 0 } "the process should log before finishing"

# Eve with only the process node reads the live log, because it is the running process's stream the node covers.
tg --token $alice.token grant $eve.user.id process_node $process | ignore
let node_only = tg --token $eve.token log $process | complete
snapshot ($node_only.stdout | redact) '
	loghello

'

# Clean up the running process.
tg --token $alice.token cancel $process $started.lease
tg --token $alice.token wait $process | ignore
