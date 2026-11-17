use ../../util/test.nu *

# A sandbox can execute an artifact that it requests while cleaning is removing the artifact's checkout.

let local = server spawn --now '2026-01-01T00:00:00Z' --config {
	advanced: { checkpoints: true }
	indexer: { cleaning: {} }
	object: { ttl: 60, ttt: 0 }
	vfs: false
}
let artifact = tg put --no-tokens 'tg.file({ "contents": tg.blob("#!/bin/sh\necho reused\n"), "executable": true })' | referent node
let unused = tg put --no-tokens 'tg.blob("unused")' | referent node
tg index
tg checkout $artifact | ignore
tg index

# Touch the artifact later, so that its checkout expires while the artifact does not.
advance_time $local 30sec
tg touch $artifact
tg index

# Hold cleaning after the index deletes the checkout and before the checkout is removed.
let delete = checkpoint watch cleaning.checkout.delete --params { checkout: $artifact }
let cleaned = checkpoint watch cleaning.object.delete --params { object: $unused }
advance_time $local 40sec
checkpoint wait $delete | ignore

# Spawn a process that requests the checkout, which no longer has an index entry, and hold the sandbox before it executes the script.
let unindexed = checkpoint watch checkout.artifact.unindexed --params { artifact: $artifact }
let start = checkpoint watch runner.process.start
let process = tg spawn --sandbox --executable $artifact | referent node
checkpoint pass $unindexed | ignore
checkpoint unwatch $unindexed

# Let cleaning remove the checkout, and run the script after cleaning finishes.
checkpoint continue $delete
checkpoint unwatch $delete
checkpoint pass $cleaned | ignore
checkpoint unwatch $cleaned
checkpoint wait $start | ignore
checkpoint unwatch $start
assert equal (tg wait $process | from json | get exit) 0
assert equal (tg log $process | str trim) 'reused'

server stop $local
