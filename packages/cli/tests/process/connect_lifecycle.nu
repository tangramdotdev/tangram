use ../../test.nu *

const driver = path self ../lib/process_connect.mjs
let server = server spawn
let idle = artifact { tangram.ts: 'export default async () => { await tg.sleep(60); };' }
let spawned = tg build --detach --verbose $idle | from json
let id = $spawned.process | split row '?' | first
let output = node $driver ($server.directory | path join socket) $id $spawned.lease idle local | complete
success $output "closing silent reads must release their subscriptions"
tg cancel $id $spawned.lease

# Process completion must preserve an open write until its end response is received.
let child = artifact { tangram.ts: 'export default async () => { await tg.sleep(60); };' }
let spawned = tg spawn --verbose $child | from json
let id = $spawned.process | split row '?' | first
let output = node $driver ($server.directory | path join socket) $id $spawned.lease write local | complete
success $output "process completion must drain pending write responses"
