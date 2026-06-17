use ../../test.nu *

# A process's status is masked from a principal without a grant, so knowing the process id is not an existence or liveness oracle.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json
let eve = tg login --verbose eve | from json

# Alice builds a private process.
let path = artifact { tangram.ts: 'export default () => 5' }
let process = tg --token $alice.token build --detach $path | str trim
tg --token $alice.token wait $process

# The owner can read the status.
let owner = tg --token $alice.token process status $process | complete
success $owner "the owner should read the process status."

# Eve must not learn the status of a process she cannot see.
let status = tg --token $eve.token process status $process | complete
failure $status "Eve must not learn the status of a process she cannot see."
assert ($status.stderr | str contains "failed to find the process") "the status should be masked as not found."
