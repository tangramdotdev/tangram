use ../lib/test.nu *

const driver = path self ../lib/process_connect.mjs
let server = server spawn
let idle = artifact { tangram.ts: 'export default async () => { await tg.sleep(60); };' }
let spawned = tg build --detach --no-tokens --verbose $idle | from json
let id = $spawned.process
let output = node $driver ($server.directory | path join socket) $id $spawned.lease idle local | complete
success $output "closing silent reads must release their subscriptions"
tg cancel $id $spawned.lease

# Process completion must preserve pending writes until their responses are received.
# Use a distinct command so this case cannot reuse the canceled process.
let child = artifact { tangram.ts: 'export default async () => { await tg.sleep(120); };' }
let spawned = tg spawn --no-tokens --verbose $child | from json
let id = $spawned.process
let output = node $driver ($server.directory | path join socket) $id $spawned.lease write local | complete
success $output "process completion must drain pending write responses"
