use ../../test.nu *

const driver = path self ../lib/process_connect.mjs
let remote = server spawn --name remote
let local = server spawn --name local --config { remotes: { default: { url: $remote.url } } }
let path = artifact { tangram.ts: 'export default async () => { await tg.sleep(60); };' }

# A confirmed detach disarms the owner of a forwarded wait before the connection closes.
let spawned = tg --url $remote.url build --detach --verbose $path | from json
let id = $spawned.process | split row '?' | first
let output = node $driver ($local.directory | path join socket) $id $spawned.lease detach | complete
success $output
sleep 200ms
assert ((tg --url $remote.url process get $id | from json | get status) != finished)
tg --url $remote.url cancel $id $spawned.lease

# Losing an observing connection does not release another handle's lease.
let spawned = tg --url $remote.url build --detach --verbose --retry $path | from json
let id = $spawned.process | split row '?' | first
let output = node $driver ($local.directory | path join socket) $id none disconnect | complete
success $output
sleep 200ms
assert ((tg --url $remote.url process get $id | from json | get status) != finished)

# Losing the owning connection cancels the process on the remote.
let output = node $driver ($local.directory | path join socket) $id $spawned.lease disconnect | complete
success $output
wait_until { (tg --url $remote.url process get $id | from json | get status) == finished }
