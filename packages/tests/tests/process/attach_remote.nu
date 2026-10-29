use ../../util/test.nu *

# A process connection can reach a process located on a remote server.

const driver = path self ../../util/process_attach.mjs
let remote = server spawn --name remote --config { advanced: { checkpoints: true } }
let local = server spawn --name local --config { remotes: { default: { url: $remote.url } } }
let path = artifact { tangram.ts: 'export default async () => { await tg.sleep(60); };' }

# A confirmed detach does not cancel the process when the connection closes.
let spawned = tg --url $remote.url build --detach --no-tokens --verbose $path | from json
let id = $spawned.process
let release = checkpoint watch process.wait.release --url $remote.url --params { process: $id }
let output = node $driver ($local.directory | path join socket) $id $spawned.lease detach | complete
success $output
checkpoint pass $release | ignore
checkpoint unwatch $release
assert ((tg --url $remote.url process get $id | from json | get status) != finished)
tg --url $remote.url cancel $id $spawned.lease

# Losing an observing connection does not release another handle's lease.
let spawned = tg --url $remote.url build --detach --no-tokens --verbose --retry $path | from json
let id = $spawned.process
let output = node $driver ($local.directory | path join socket) $id none disconnect | complete
success $output
assert ((tg --url $remote.url process get $id | from json | get status) != finished)

# Losing the owning connection cancels the process on the remote.
let output = node $driver ($local.directory | path join socket) $id $spawned.lease disconnect | complete
success $output
timeout 30s tg --url $remote.url process wait $id | complete | ignore
assert equal (tg --url $remote.url process get $id | from json | get status) finished
