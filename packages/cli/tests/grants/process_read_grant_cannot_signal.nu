use ../../test.nu *

# Granting process_node to a principal should not allow that prinicipal to singal the process.

let server = spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice runs a long-running, non-cacheable process (signalling rejects cacheable processes).
let path = artifact { tangram.ts: 'export default async function () { await tg.sleep(30); return "done"; }' }
let process = tg --token $alice.token run --network=true --detach $path | str trim

# Without any grant, Eve cannot see or signal the process; it is masked as not found.
let masked = tg --token $eve.token process signal $process --signal KILL | complete
failure $masked "Eve should not signal a process she cannot see."

# Alice grants Eve only read (process_node) on the process.
tg --token $alice.token grant $eve.user.id process_node $process

# A read grant must not confer the ability to signal (control) the running process.
let signaled = tg --token $eve.token process signal $process --signal KILL | complete
failure $signaled "a read grant must not confer the ability to signal the process."

# The process must remain running, not be terminated by Eve's signal.
let status = tg --token $alice.token process status $process | from json | get 0
assert ($status == "started") "Eve's signal must not have terminated the process."
