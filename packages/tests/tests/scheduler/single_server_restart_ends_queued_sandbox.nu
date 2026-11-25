use ../../util/test.nu *

# A process with a new sandbox that is still queued when a single server crashes does not run after the restart. Its sandbox, whose ttl is zero, is destroyed, and the process finishes with the restart's error.

let server = server spawn --config {
	runner: { cpus: 1, overcommit: { cpu: 1 } },
	scheduler: { default_cpu: 1 },
}

# Fill the runner, then spawn a process with a new sandbox that waits in the queue.
tg sandbox create --wait --no-tokens --cpu 1 | ignore
let process = tg spawn --no-tokens --sandbox --executable /bin/sh -- -c 'exit 0' | referent node
tg index
let data = tg process get $process | from json
assert equal $data.status queued 'the process should be queued'
let sandbox = $data.sandbox

# Crash the server, and start it again.
let pid = server_pid $server.directory
kill --signal 9 $pid
wait_for_exit $pid
let server = server start $server

# The queued sandbox is destroyed, and the process finishes with the restart's error without running.
success (timeout 30s tg sandbox status --until destroyed $sandbox | complete) 'the sandbox should be destroyed'
let sandbox_data = tg sandbox get $sandbox | from json | get data
assert equal $sandbox_data.error.message 'server restarted'
tg index
let data = tg process get $process | from json
assert equal $data.status finished
assert equal $data.exit 1
assert equal $data.error.node.message 'server restarted'
assert equal $data.started_at? null 'the process should not have started'
