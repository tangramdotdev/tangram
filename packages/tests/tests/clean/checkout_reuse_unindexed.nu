use ../../util/test.nu *

# A checkout whose index write was lost keeps its dependency when it is reused, so a sandbox can execute it after the dependency's original ttl expires.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	indexer: { cleaning: {} }
	object: { ttl: 60, ttt: 0 }
	vfs: false
}
let artifact = tg put --no-tokens 'tg.symlink({ "artifact": tg.file({ "contents": tg.blob("#!/bin/sh\necho reused\n"), "executable": true }) })' | referent node
let unused = tg put --no-tokens 'tg.blob("unused")' | referent node
tg index

# Check out the artifact and kill the server while the index writes of its checkouts are held.
checkpoint watch index.write | ignore
tg checkout $artifact | ignore
let pid = open ($local.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let local = server start $local

# Reuse the checkouts, which exist without index entries.
tg checkout $artifact | ignore
tg index

# Reuse the checkouts again and hold the sandbox before it executes the script.
advance_time $local 50sec
let start = checkpoint watch runner.process.start
let process = tg spawn --sandbox --executable $artifact | referent node
checkpoint wait $start | ignore

# Run the script after cleaning has passed the dependency's original expiry time.
let delete = checkpoint watch cleaning.object.delete --params { object: $unused }
advance_time $local 20sec
checkpoint wait $delete | ignore
checkpoint unwatch $delete
checkpoint unwatch $start
assert equal (tg wait $process | from json | get exit) 0
assert equal (tg log $process | str trim) 'reused'

server stop $local
