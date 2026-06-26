use ../../test.nu *

# A public build grants public read, not control: another principal may read a public process but must not signal or cancel it.

let server = spawn --config { authentication: { providers: { insecure: true } } }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice runs a long-running process publicly.
let path = artifact { tangram.ts: 'export default async function () { await tg.sleep(30); return "done"; }' }
let process = tg --token $alice.token run --network=true --detach --public $path | str trim

# Eve can read the public process.
let read = tg --token $eve.token get $process | complete
success $read "Eve should read a public process."

# Eve must not signal the public process; public grants read, not control.
let signaled = tg --token $eve.token process signal $process --signal KILL | complete
failure $signaled "Eve must not signal a public process."

# The process is still running, not terminated by Eve.
let status = tg --token $alice.token process status $process | from json | get 0
assert ($status == "started") "Eve's signal must not have terminated the public process."
