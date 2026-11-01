use ../../util/test.nu *

# By default, tg ps lists the started processes that the current principal created, and a finished process moves to the finished status.

let local = server spawn --config { authentication: { users: { providers: { insecure: true } } } }

let alice = tg login --verbose --name alice | from json
let eve = tg login --verbose --name eve | from json

let path = artifact {
	tangram.ts: 'export default async () => { console.log("ready"); await tg.sleep(120); };',
}

# Spawn a process that runs until it is canceled.
let spawned = tg --token $alice.token spawn --verbose $path | from json
let process = $spawned.process
let log = timeout 30s tg --token $alice.token process log --no-timeout --length 6 $process | str trim
assert equal $log ready
tg index

# The process records its creator and owner.
let data = tg --token $alice.token process get $process | from json
assert equal $data.creator $alice.user.id "the process creator should be the user who spawned it"
assert equal $data.owner $alice.user.id "the process owner should default to the user who spawned it"

# Alice sees the started process, and Eve does not.
let alice_list = tg --token $alice.token ps | from json
assert (($alice_list | where node == $process | is-empty) == false) "Alice should see her started process"
let eve_list = tg --token $eve.token ps | from json
assert ($eve_list | where node == $process | is-empty) "Eve must not see Alice's process"

# Cancel the process and wait for it to finish.
tg --token $alice.token cancel $process $spawned.lease
tg --token $alice.token process wait $process | complete | ignore
tg index

# The finished process is no longer listed by default, and is listed with the finished status.
let started = tg --token $alice.token ps | from json
assert ($started | where node == $process | is-empty) "a finished process should not be listed by default"
let finished = tg --token $alice.token ps --status finished | from json
assert (($finished | where node == $process | is-empty) == false) "a finished process should be listed with the finished status"
let any = tg --token $alice.token ps --any-status | from json
assert (($any | where node == $process | is-empty) == false) "a finished process should be listed with any status"
