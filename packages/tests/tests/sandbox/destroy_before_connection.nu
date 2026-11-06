use ../../util/test.nu *

# A sandbox can be read and destroyed straight after it is created, before any runner has connected it.

let local = server spawn --config {
	roles: [api indexer scheduler],
	scheduler: { unplaceable_timeout: null },
}

# Creating returns once the scheduler accepts the sandbox.
let sandbox = tg sandbox create --no-tokens | referent node
assert equal (tg sandbox get $sandbox | from json | get data.status) starting

# Destroying a queued sandbox removes it from the queue.
tg sandbox destroy --wait $sandbox
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed
success (tg sandbox destroy $sandbox | complete) "destroying a destroyed sandbox should have no effect"
