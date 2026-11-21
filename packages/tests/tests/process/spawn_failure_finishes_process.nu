use ../../util/test.nu *

# A spawn into a sandbox that is destroyed returns the process, which finishes with the failure for its creator, with no process queued.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let alice = tg login --verbose --name alice | from json

# Alice destroys her sandbox, and then spawns a process in it.
let sandbox = tg --token $alice.token sandbox create --no-tokens | referent node
tg --token $alice.token sandbox destroy $sandbox
tg --token $alice.token sandbox wait $sandbox | ignore
let spawned = tg --token $alice.token spawn --no-tokens --verbose $'--sandbox=($sandbox)' --executable /bin/sh -- -c 'exit 0' | from json
let process = $spawned.process
let outcome = tg --token $alice.token wait $process | from json
assert equal $outcome.exit 1
tg index

# Alice's process is finished with the failure, and none of her processes is queued.
let queued = tg --token $alice.token ps --status queued | from json
assert ($queued | is-empty) 'no process should be queued'
let finished = tg --token $alice.token ps --status finished | from json
assert equal ($finished | length) 1 'the process should be finished'
assert equal ($finished | first | get node) $process
let data = tg --token $alice.token process get $process | from json
assert equal $data.status finished
assert equal $data.exit 1
assert ($data.error? != null) 'the process should report the failure'
