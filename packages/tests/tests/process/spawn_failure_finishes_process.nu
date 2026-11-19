use ../../util/test.nu *

# A spawn into a sandbox that is destroyed fails, and leaves the process finished with the failure for its creator, with no process queued.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json

# Alice destroys her sandbox, and then spawns a process in it.
let sandbox = tg --token $alice.token sandbox create --no-tokens | referent node
tg --token $alice.token sandbox destroy $sandbox
tg --token $alice.token sandbox wait $sandbox | ignore
let output = tg --token $alice.token spawn --no-tokens $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'exit 0' | complete
failure $output 'the spawn should fail'
tg index

# Alice's process is finished with the failure, and none of her processes is queued.
let queued = tg --token $alice.token ps --status queued | from json
assert ($queued | is-empty) 'no process should be queued'
let finished = tg --token $alice.token ps --status finished | from json
assert equal ($finished | length) 1 'the process should be finished'
let process = $finished | first | get node
let data = tg --token $alice.token process get $process | from json
assert equal $data.status finished
assert equal $data.exit 1
assert ($data.error? != null) 'the process should report the failure'
