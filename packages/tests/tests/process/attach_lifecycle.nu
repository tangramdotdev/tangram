use ../../lib/test.nu *

# Process connection setup and teardown follow the process lifecycle.

const driver = path self ../../lib/process_attach.mjs
let local = server spawn
let idle = artifact { tangram.ts: 'export default async () => { await tg.sleep(60); };' }
let spawned = tg build --detach --no-tokens --verbose $idle | from json
let id = $spawned.process
let output = node $driver ($local.directory | path join socket) $id $spawned.lease idle local | complete
success $output "detaching must close the connection"
tg cancel $id $spawned.lease
