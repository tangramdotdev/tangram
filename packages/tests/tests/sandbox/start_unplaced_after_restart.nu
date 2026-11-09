use ../../util/test.nu *

# A sandbox that is still queued when a single server crashes is stopped, and it starts on a runner with no directory for it.

let local = server spawn --config {
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1 },
}

# Fill the runner, then create a second sandbox that waits in the queue.
let running = tg sandbox create --wait --no-tokens --cpu 1 | referent node
let queued = tg sandbox create --no-tokens --cpu 1 | referent node

# Kill the server while the second sandbox is queued, and start it again.
let pid = open ($local.directory | path join 'lock') | into int
kill --signal 9 $pid
wait_for_exit $pid
let local = server start $local

# The restarted server stops both sandboxes, and the queued sandbox starts on a runner with no directory for it.
success (timeout 60s tg sandbox start --wait $queued | complete) "the sandbox that was stopped before it was placed should start"
assert equal (tg sandbox get $queued | from json | get data.status) started
