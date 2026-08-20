use ../../test.nu *

# A process_parent grant permits signaling a running non-cacheable process.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

let path = artifact { tangram.ts: 'export default async () => { await tg.sleep(30); return "done"; }' }
let process = tg --token $alice.token run --network=true --detach $path | str trim

tg --token $alice.token grant $eve.user.id process_parent $process
tg --token $alice.token index

let signaled = tg --token $eve.token process signal $process --signal KILL | complete
success $signaled "a process_parent grant should permit signaling the process."
